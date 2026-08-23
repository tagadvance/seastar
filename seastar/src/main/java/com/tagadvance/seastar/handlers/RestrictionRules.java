package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/**
 * Cassandra's rules about which WHERE clauses a table will answer, applied once for every statement
 * that has one so that SELECT, UPDATE and DELETE cannot drift apart.
 *
 * <p>The rules are not a single "is this column part of the primary key" test. They are, verified
 * against a live cluster:
 *
 * <ul>
 *   <li>A relation is never {@code !=}, {@code IS NOT NULL}, {@code LIKE} or {@code ANN OF};
 *       {@code CONTAINS} needs a collection and {@code CONTAINS KEY} a map; a multi-column relation
 *       names clustering columns only, in key order.
 *   <li>A column carrying an equality carries nothing else, and no column carries two lower or two
 *       upper bounds.
 *   <li>Clustering columns are restricted left to right with no gap, and a relation that does not
 *       pin its columns stops the ones after it.
 *   <li>SELECT wants the partition key pinned by {@code =} or {@code IN} and every other column
 *       either part of the primary key or answered by an index; anything else is a scan, and a scan
 *       needs ALLOW FILTERING. ALLOW FILTERING waives the clustering rules too.
 *   <li>UPDATE wants the whole primary key pinned by {@code =} or {@code IN} - a range writes to
 *       rows it cannot name.
 *   <li>DELETE wants only the partition key pinned; clustering columns may be partly restricted,
 *       omitted, or given a range, because a partition-level delete is ordinary Cassandra. Naming
 *       columns to clear, or attaching an {@code IF}, pulls it back to a single row.
 * </ul>
 */
final class RestrictionRules {

	private static final String FILTERING =
		"Cannot execute this query as it might involve data filtering and thus may have "
			+ "unpredictable performance. If you want to execute this query despite the performance "
			+ "unpredictability, use ALLOW FILTERING";

	private RestrictionRules() {
		// hidden constructor
	}

	/**
	 * The test a row has to pass to be returned, or null when the statement restricts nothing.
	 */
	static @Nullable Predicate<SeaStarRow> forSelect(final Query query, final Node coordinator) {
		final var restrictions = query.restrictions();
		if (restrictions.isEmpty()) {
			return null;
		}

		final var target = query.target();
		validateRelations(target, restrictions, coordinator);
		validateOneRelationPerColumn(restrictions, coordinator);
		// ALLOW FILTERING is Cassandra's way of saying "scan", and a scan has no clustering prefix
		// to respect.
		if (!query.allowFiltering()) {
			validateClusteringPrefix(target, restrictions, coordinator);
			requireNoFiltering(target, restrictions, coordinator);
		}

		return predicate(restrictions);
	}

	/**
	 * The rows an UPDATE writes to. An UPDATE addresses one row per combination of key values, so
	 * the whole primary key has to be pinned.
	 */
	static Predicate<SeaStarRow> forUpdate(final Modification update, final Node coordinator) {
		final var target = update.target();
		final var restrictions = update.restrictions();
		validateRelations(target, restrictions, coordinator);
		validateOneRelationPerColumn(restrictions, coordinator);
		requirePinnedPartitionKey(target, restrictions, coordinator, "UPDATE");
		requireOnlyPrimaryKeyColumns(target, restrictions, coordinator);
		requirePinnedClustering(target, restrictions, coordinator,
			"Slice restrictions are not supported on the clustering columns in UPDATE statements");
		// An UPDATE that writes only static columns addresses the partition, so it neither needs the
		// clustering key nor may name it; the handler is what refuses a clustering restriction there.
		if (!update.writesOnlyStaticColumns()) {
			requireWholeClusteringKey(target, restrictions, coordinator);
		}
		if (isConditional(update)) {
			rejectPartitionKeyIn(target, restrictions, coordinator, "updates");
		}

		return predicate(restrictions);
	}

	/**
	 * The rows a DELETE removes from or clears columns in.
	 */
	static Predicate<SeaStarRow> forDelete(final Modification delete, final Node coordinator) {
		final var target = delete.target();
		final var restrictions = delete.restrictions();
		validateRelations(target, restrictions, coordinator);
		validateOneRelationPerColumn(restrictions, coordinator);
		requirePinnedPartitionKey(target, restrictions, coordinator, "DELETE");
		requireOnlyPrimaryKeyColumns(target, restrictions, coordinator);
		validateClusteringPrefix(target, restrictions, coordinator);
		// Clearing named columns writes into rows rather than removing them, so it has to name them -
		// unless every column it clears is static, which belongs to the partition rather than a row.
		if (!delete.assignments().isEmpty() && !delete.writesOnlyStaticColumns()
			&& !pinsPrimaryKey(target, restrictions, false)) {
			throw new InvalidQueryException(coordinator,
				"Range deletions are not supported for specific columns");
		}
		if (isConditional(delete)) {
			rejectPartitionKeyIn(target, restrictions, coordinator, "deletions");
			if (!delete.writesOnlyStaticColumns() && !pinsPrimaryKey(target, restrictions, true)) {
				throw new InvalidQueryException(coordinator,
					"DELETE statements must restrict all PRIMARY KEY columns with equality relations "
						+ "in order to delete non static columns");
			}
		}

		return predicate(restrictions);
	}

	/**
	 * The value an equality restriction pins each primary key column to, by column position, or null
	 * when the WHERE clause does not pin the whole primary key to a single row. An UPDATE uses it to
	 * synthesize the row it upserts when none matches.
	 */
	static @Nullable Map<Integer, Object> upsertKey(final Modification update) {
		final Map<Integer, Object> pinned = new LinkedHashMap<>();
		update.restrictions().forEach(restriction -> pinned.putAll(restriction.equalityValues()));

		return pinned.size() == update.target().primaryKeyNames().size() ? pinned : null;
	}

	/**
	 * How many partitions an {@code IN} on the partition key may be expanded to before scanning the
	 * table is the cheaper answer. Well beyond any WHERE clause a test writes.
	 */
	private static final int MAX_PARTITIONS = 1_024;

	/**
	 * The partitions a WHERE clause reaches, or null when it does not pin every partition key column
	 * and so has to be answered by a scan.
	 *
	 * <p>Each element is one partition's key values, in key order - the key {@link SeaStarTable} is
	 * indexed by. {@code IN} pins a column to several values at once, so the answer is the product of
	 * the alternatives rather than a single key, and it is distinct: {@code WHERE pk IN (1, 1)}
	 * reaches one partition, and reading it twice would answer with each of its rows twice.
	 */
	static @Nullable List<List<Object>> partitions(final Target target,
		final List<Restriction> restrictions) {
		final var partitionKey = target.partitionKeyNames();
		final Map<CqlIdentifier, List<Object>> pinned = new LinkedHashMap<>();
		for (final var restriction : restrictions) {
			// A multi-column relation names clustering columns only, so it never pins a partition.
			if (!restriction.operator().isEquality() || restriction.isMultiColumn()
				|| !partitionKey.contains(restriction.column().name())) {
				continue;
			}
			pinned.put(restriction.column().name(), restriction.values()
				.stream()
				.map(tuple -> tuple.get(0))
				.distinct()
				.toList());
		}
		if (!pinned.keySet().containsAll(partitionKey)) {
			return null;
		}

		List<List<Object>> keys = List.of(List.of());
		for (final var column : partitionKey) {
			final var alternatives = pinned.get(column);
			if (keys.size() * (long) alternatives.size() > MAX_PARTITIONS) {
				return null;
			}
			keys = keys.stream()
				.flatMap(prefix -> alternatives.stream().map(value -> {
					// Not List.copyOf: a key is a positional list of values and must tolerate a null in
					// it. A null pinning a partition key column is refused by validateRelations, which
					// every caller runs first, so it cannot be one of them.
					final List<Object> key = new ArrayList<>(prefix);
					key.add(value);

					return Collections.unmodifiableList(key);
				}))
				.toList();
		}

		return keys;
	}

	/**
	 * The rows a statement has to look at: the partitions its WHERE clause reaches, or the whole
	 * table when it reaches none in particular.
	 */
	static Stream<SeaStarRow> rows(final SeaStarTable table,
		final @Nullable List<List<Object>> partitions) {
		return partitions == null ? table.rows() : partitions.stream().flatMap(table::partition);
	}

	private static boolean isConditional(final Modification modification) {
		return modification.ifExists() || !modification.conditions().isEmpty();
	}

	private static void validateRelations(final Target target, final List<Restriction> restrictions,
		final Node coordinator) {
		final var codecRegistry = target.table().context().getCodecRegistry();
		final var clustering = clusteringNames(target);
		final var primaryKey = target.primaryKeyNames();
		for (final var restriction : restrictions) {
			if (restriction.isMultiColumn()) {
				validateMultiColumn(restriction, clustering, coordinator);
			}
			if (restriction.operator().isEquality() && !restriction.isMultiColumn()) {
				requireNonNullKey(restriction, primaryKey, coordinator);
			}
			switch (restriction.operator()) {
				case NEQ -> throw new InvalidQueryException(coordinator,
					"Unsupported \"!=\" relation on column %s".formatted(label(restriction)));
				case IS_NOT -> throw new InvalidQueryException(coordinator,
					"Unsupported restriction: %s IS NOT NULL".formatted(label(restriction)));
				// SeaStar models secondary indexes but not the SASI index a LIKE needs, so there is
				// no table on which one could be answered.
				case LIKE -> throw new InvalidQueryException(coordinator,
					"LIKE restriction is only supported on properly indexed columns, and %s is not one"
						.formatted(label(restriction)));
				case ANN -> throw new InvalidQueryException(coordinator,
					"ANN ordering is only supported on vector-indexed columns, and %s is not one"
						.formatted(label(restriction)));
				default -> {
					// The comparisons need no check of their own beyond an orderable column.
				}
			}
			if (restriction.operator().isSlice()) {
				restriction.columns().stream()
					.filter(column -> !Restriction.isOrderable(column.type(), codecRegistry))
					.findFirst()
					.ifPresent(column -> {
						throw new InvalidQueryException(coordinator,
							"Slice restrictions are not supported on column %s of type %s".formatted(
								column.name().asInternal(), column.type().asCql(true, true)));
					});
			}
		}
	}

	/**
	 * A primary key column compared to null names no row, and Cassandra refuses the comparison rather
	 * than answering nothing - which is what makes a bound value nobody supplied an error rather than
	 * a silent miss. Only {@code =} and {@code IN} are refused; a null slice bound is accepted, and a
	 * null on a column outside the primary key is reported by the filtering path instead.
	 */
	private static void requireNonNullKey(final Restriction restriction,
		final Set<CqlIdentifier> primaryKey, final Node coordinator) {
		final var column = restriction.column();
		final var nulls = restriction.values().stream()
			.flatMap(List::stream)
			.anyMatch(Objects::isNull);
		if (primaryKey.contains(column.name()) && nulls) {
			throw new InvalidQueryException(coordinator,
				"Invalid null value in condition for column %s".formatted(column.name().asInternal()));
		}
	}

	private static void validateMultiColumn(final Restriction restriction,
		final List<CqlIdentifier> clustering, final Node coordinator) {
		final var names = names(restriction);
		for (final var name : names) {
			if (!clustering.contains(name)) {
				throw new InvalidQueryException(coordinator,
					("Multi-column relations can only be applied to clustering columns but was "
						+ "applied to: %s").formatted(name.asInternal()));
			}
		}
		for (int i = 1; i < names.size(); i++) {
			if (clustering.indexOf(names.get(i)) <= clustering.indexOf(names.get(i - 1))) {
				throw new InvalidQueryException(coordinator,
					("Clustering columns must appear in the PRIMARY KEY order in multi-column "
						+ "relations: %s").formatted(label(restriction)));
			}
		}
	}

	private static void validateOneRelationPerColumn(final List<Restriction> restrictions,
		final Node coordinator) {
		final Map<List<CqlIdentifier>, List<Restriction>> grouped = restrictions.stream()
			.collect(Collectors.groupingBy(RestrictionRules::names, LinkedHashMap::new,
				Collectors.toList()));
		grouped.forEach((names, group) -> {
			final var label = names.stream().map(CqlIdentifier::asInternal)
				.collect(Collectors.joining(", "));
			if (group.size() > 1 && group.stream()
				.anyMatch(restriction -> restriction.operator() == CqlOperator.EQ)) {
				throw new InvalidQueryException(coordinator,
					"%s cannot be restricted by more than one relation if it includes an Equal"
						.formatted(label));
			}
			if (bounds(group, CqlOperator.GT, CqlOperator.GTE) > 1) {
				throw new InvalidQueryException(coordinator,
					"More than one restriction was found for the start bound on %s".formatted(label));
			}
			if (bounds(group, CqlOperator.LT, CqlOperator.LTE) > 1) {
				throw new InvalidQueryException(coordinator,
					"More than one restriction was found for the end bound on %s".formatted(label));
			}
		});
	}

	private static long bounds(final List<Restriction> group, final CqlOperator strict,
		final CqlOperator inclusive) {
		return group.stream()
			.filter(restriction -> restriction.operator() == strict
				|| restriction.operator() == inclusive)
			.count();
	}

	/**
	 * Clustering columns are restricted left to right: a column may only be restricted once every
	 * column before it is, and a relation that does not pin its columns to a value stops every
	 * column after the ones it names.
	 */
	private static void validateClusteringPrefix(final Target target,
		final List<Restriction> restrictions, final Node coordinator) {
		final var clustering = clusteringNames(target);
		for (int i = 1; i < clustering.size(); i++) {
			final var name = clustering.get(i);
			if (!isRestricted(restrictions, name)) {
				continue;
			}
			final var previous = clustering.get(i - 1);
			if (!isRestricted(restrictions, previous)) {
				throw new InvalidQueryException(coordinator,
					("PRIMARY KEY column \"%s\" cannot be restricted as preceding column \"%s\" is "
						+ "not restricted").formatted(name.asInternal(), previous.asInternal()));
			}
			// A multi-column relation restricts its own columns as a unit, so a slice over
			// (ck1, ck2) does not stop ck2.
			final var stopped = restrictions.stream()
				.filter(restriction -> !restriction.operator().isEquality())
				.filter(restriction -> names(restriction).contains(previous))
				.anyMatch(restriction -> !names(restriction).contains(name));
			if (stopped) {
				throw new InvalidQueryException(coordinator,
					("Clustering column \"%s\" cannot be restricted (preceding column \"%s\" is "
						+ "restricted by a non-EQ relation)").formatted(name.asInternal(),
						previous.asInternal()));
			}
		}
	}

	/**
	 * A query that does not go straight to a partition, and does not answer every remaining relation
	 * from an index, has to read rows it will not return - which Cassandra refuses to do unquietly.
	 */
	private static void requireNoFiltering(final Target target,
		final List<Restriction> restrictions, final Node coordinator) {
		final var partitionKey = target.partitionKeyNames();
		final var primaryKey = target.primaryKeyNames();
		final var indexed = indexedColumns(target.table());
		// The query reaches one partition only when every partition key column is pinned by = or IN.
		final var partition = restrictedNames(restrictions).containsAll(partitionKey)
			&& restrictions.stream()
			.filter(restriction -> names(restriction).stream().anyMatch(partitionKey::contains))
			.allMatch(restriction -> restriction.operator().isEquality());

		for (final var restriction : restrictions) {
			// A secondary index answers an equality on its column without a scan; anything else on
			// a non-key column is one. Without a partition to walk, every relation has to be one an
			// index answers.
			final var indexedEquality = restriction.operator() == CqlOperator.EQ
				&& !restriction.isMultiColumn() && indexed.contains(restriction.column().name());
			final var reachable = indexedEquality
				|| (partition && names(restriction).stream().allMatch(primaryKey::contains));
			if (!reachable) {
				throw new InvalidQueryException(coordinator, FILTERING);
			}
		}
	}

	private static void requirePinnedPartitionKey(final Target target,
		final List<Restriction> restrictions, final Node coordinator, final String statement) {
		final var partitionKey = target.partitionKeyNames();
		final var restricted = restrictedNames(restrictions);
		final var missing = partitionKey.stream()
			.filter(name -> !restricted.contains(name))
			.map(CqlIdentifier::asInternal)
			.toList();
		if (!missing.isEmpty()) {
			throw new InvalidQueryException(coordinator,
				"Some partition key parts are missing: %s".formatted(String.join(", ", missing)));
		}

		final var sliced = restrictions.stream()
			.filter(restriction -> names(restriction).stream().anyMatch(partitionKey::contains))
			.anyMatch(restriction -> !restriction.operator().isEquality());
		if (sliced) {
			throw new InvalidQueryException(coordinator,
				("Only EQ and IN relation are supported on the partition key (unless you use the "
					+ "token() function) for %s statements").formatted(statement));
		}
	}

	private static void requireOnlyPrimaryKeyColumns(final Target target,
		final List<Restriction> restrictions, final Node coordinator) {
		final var primaryKey = target.primaryKeyNames();
		final var offending = restrictedNames(restrictions).stream()
			.filter(name -> !primaryKey.contains(name))
			.map(CqlIdentifier::asInternal)
			.toList();
		if (!offending.isEmpty()) {
			throw new InvalidQueryException(coordinator,
				"Non PRIMARY KEY columns found in where clause: %s".formatted(
					String.join(", ", offending)));
		}
	}

	private static void requirePinnedClustering(final Target target,
		final List<Restriction> restrictions, final Node coordinator, final String message) {
		final var clustering = clusteringNames(target);
		final var sliced = restrictions.stream()
			.filter(restriction -> names(restriction).stream().anyMatch(clustering::contains))
			.anyMatch(restriction -> !restriction.operator().isEquality());
		if (sliced) {
			throw new InvalidQueryException(coordinator, message);
		}
	}

	private static void requireWholeClusteringKey(final Target target,
		final List<Restriction> restrictions, final Node coordinator) {
		final var restricted = restrictedNames(restrictions);
		final var missing = clusteringNames(target).stream()
			.filter(name -> !restricted.contains(name))
			.map(CqlIdentifier::asInternal)
			.toList();
		if (!missing.isEmpty()) {
			throw new InvalidQueryException(coordinator,
				"Some clustering keys are missing: %s".formatted(String.join(", ", missing)));
		}
	}

	private static void rejectPartitionKeyIn(final Target target,
		final List<Restriction> restrictions, final Node coordinator, final String kind) {
		final var partitionKey = target.partitionKeyNames();
		final var in = restrictions.stream()
			.filter(restriction -> restriction.operator() == CqlOperator.IN)
			.anyMatch(restriction -> names(restriction).stream().anyMatch(partitionKey::contains));
		if (in) {
			throw new InvalidQueryException(coordinator,
				"IN on the partition key is not supported with conditional %s".formatted(kind));
		}
	}

	/**
	 * Whether the restrictions name every primary key column exactly, either by {@code =} alone or
	 * by {@code =} and {@code IN}.
	 */
	private static boolean pinsPrimaryKey(final Target target, final List<Restriction> restrictions,
		final boolean equalsOnly) {
		final Set<CqlIdentifier> pinned = restrictions.stream()
			.filter(restriction -> equalsOnly ? restriction.operator() == CqlOperator.EQ
				: restriction.operator().isEquality())
			.flatMap(restriction -> names(restriction).stream())
			.collect(Collectors.toCollection(HashSet::new));

		return pinned.containsAll(target.primaryKeyNames());
	}

	private static Predicate<SeaStarRow> predicate(final List<Restriction> restrictions) {
		return restrictions.stream()
			.map(Restriction::toPredicate)
			.reduce(Predicate::and)
			.orElseThrow(() -> new IllegalStateException(
				"a WHERE clause with relations must yield at least one predicate"));
	}

	private static boolean isRestricted(final List<Restriction> restrictions,
		final CqlIdentifier name) {
		return restrictions.stream()
			.anyMatch(restriction -> names(restriction).contains(name));
	}

	private static List<CqlIdentifier> names(final Restriction restriction) {
		return restriction.columns().stream().map(Restriction.Column::name).toList();
	}

	private static String label(final Restriction restriction) {
		return names(restriction).stream()
			.map(CqlIdentifier::asInternal)
			.collect(Collectors.joining(", "));
	}

	private static Set<CqlIdentifier> restrictedNames(final List<Restriction> restrictions) {
		return restrictions.stream()
			.flatMap(restriction -> names(restriction).stream())
			.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	private static List<CqlIdentifier> clusteringNames(final Target target) {
		return target.table().getClusteringColumns().keySet().stream().map(ColumnMetadata::getName)
			.toList();
	}

	private static Set<CqlIdentifier> indexedColumns(final SeaStarTable table) {
		return table.getIndexes()
			.values()
			.stream()
			.map(index -> CqlIdentifier.fromInternal(indexTargetColumn(index.getTarget())))
			.collect(Collectors.toCollection(HashSet::new));
	}

	/**
	 * Collection index targets look like {@code values(col)}, {@code keys(col)},
	 * {@code entries(col)} or {@code full(col)}; a simple index target is just the column name.
	 */
	private static String indexTargetColumn(final String target) {
		final var open = target.indexOf('(');
		if (open >= 0 && target.endsWith(")")) {
			return target.substring(open + 1, target.length() - 1);
		}

		return target;
	}

}
