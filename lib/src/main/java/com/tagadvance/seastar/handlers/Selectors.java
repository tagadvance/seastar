package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarTable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.jspecify.annotations.Nullable;

/**
 * Translates a SELECT clause into {@link Selector}s. Part of the boundary {@link Queries} draws:
 * below it the {@code org.apache.cassandra} parse tree, above it a list of result columns and a way
 * to compute each one.
 *
 * <p>What a selector is <em>allowed</em> to be stays here rather than in the handler, because every
 * rule is about the statement and the column types it names: an aggregate over a column that has no
 * order, a {@code writetime} of a primary key part, a cast between types that have no conversion.
 */
final class Selectors {

	/**
	 * The keyspace a cluster reports an aggregate under, which is what makes {@code count(v)} come
	 * back as {@code system.count(v)} while {@code ttl(v)} comes back unqualified.
	 */
	private static final String SYSTEM = "system";

	/**
	 * The types {@code sum} and {@code avg} have a signature for. Cassandra names them all when a
	 * call matches none, and the list is the reason: they are the types addition is defined over.
	 */
	private static final Set<DataType> NUMERIC = Set.of(DataTypes.TINYINT, DataTypes.SMALLINT,
		DataTypes.INT, DataTypes.BIGINT, DataTypes.VARINT, DataTypes.FLOAT, DataTypes.DOUBLE,
		DataTypes.DECIMAL, DataTypes.COUNTER);

	private Selectors() {
		// hidden constructor
	}

	/**
	 * The SELECT clause as written, or an empty list for {@code SELECT *}, which selects the table as
	 * it stands rather than a fixed set of columns.
	 */
	static List<Selector> translate(final SeaStarTable table, final List<RawSelector> selectClause,
		final CodecRegistry codecRegistry, final ProtocolVersion version, final Node coordinator) {
		final List<Selector> selectors = new ArrayList<>(selectClause.size());
		for (final var raw : selectClause) {
			final var selector = selector(table, raw.selectable, codecRegistry, version, coordinator);
			selectors.add(raw.alias == null ? selector
				: alias(selector, CqlIdentifier.fromInternal(raw.alias.toString())));
		}

		return List.copyOf(selectors);
	}

	private static Selector alias(final Selector selector, final CqlIdentifier name) {
		return new Selector(selector.keyspace(), selector.table(), name, selector.type(),
			selector.columnIndex(), selector.aggregate(), selector.reader());
	}

	private static Selector selector(final SeaStarTable table, final Selectable.Raw raw,
		final CodecRegistry codecRegistry, final ProtocolVersion version, final Node coordinator) {
		if (raw instanceof Selectable.RawIdentifier identifier) {
			return column(table, Selectables.toIdentifier(identifier), coordinator);
		}
		if (raw instanceof Selectable.WithFunction.Raw function) {
			return function(table, function, codecRegistry, version, coordinator);
		}
		if (raw instanceof Selectable.WritetimeOrTTL.Raw writetime) {
			return writetimeOrTtl(table, writetime, coordinator);
		}
		if (raw instanceof Selectable.WithCast.Raw cast) {
			return cast(table, cast, codecRegistry, version, coordinator);
		}

		throw new InvalidQueryException(coordinator,
			"SeaStar does not support %s in a SELECT clause".formatted(construct(raw)));
	}

	/**
	 * The CQL name of a select-clause construct, read off its parse-tree class so that the internal
	 * class never reaches the message: {@code Selectable$WithElementSelection$Raw} is reported as
	 * {@code element selection}.
	 */
	private static String construct(final Selectable.Raw raw) {
		final var simple = raw.getClass().getName();
		final var start = simple.lastIndexOf("Selectable$") + "Selectable$".length();
		final var name = simple.substring(start).replace("$Raw", "").replace('$', ' ');

		return name.replaceFirst("^With", "")
			.replaceAll("(?<=[a-z0-9])(?=[A-Z])", " ")
			.toLowerCase(Locale.ROOT);
	}

	private static Selector column(final SeaStarTable table, final CqlIdentifier name,
		final Node coordinator) {
		final var index = requireColumn(table, name, coordinator);
		final var type = table.get(index).getType();

		return new Selector(table.getKeyspace(), table.getName(), name, type, index, null,
			row -> row.getObject(index));
	}

	private static int requireColumn(final SeaStarTable table, final CqlIdentifier name,
		final Node coordinator) {
		final var index = table.firstIndexOf(name);
		if (index < 0) {
			throw new InvalidQueryException(coordinator,
				"Undefined column name %s in table %s.%s".formatted(name.asInternal(),
					table.getKeyspace().asInternal(), table.getName().asInternal()));
		}

		return index;
	}

	private static Selector function(final SeaStarTable table,
		final Selectable.WithFunction.Raw raw, final CodecRegistry codecRegistry,
		final ProtocolVersion version, final Node coordinator) {
		final var name = FieldBindings.SELECTABLE_FUNCTION_NAME.require(raw).name.toLowerCase(
			Locale.ROOT);
		final var args = FieldBindings.SELECTABLE_FUNCTION_ARGS.require(raw);

		if ("count_rows".equals(name)) {
			// SELECT count(*). The parser rewrites the star into a nullary function, and a cluster
			// reports the column as a bare "count".
			return aggregate(table, CqlIdentifier.fromInternal("count"), DataTypes.BIGINT,
				Selector.Aggregate.COUNT_ROWS, row -> Boolean.TRUE);
		}
		if ("token".equals(name)) {
			return token(table, args, codecRegistry, version, coordinator);
		}

		final var aggregate = switch (name) {
			case "count" -> Selector.Aggregate.COUNT;
			case "min" -> Selector.Aggregate.MIN;
			case "max" -> Selector.Aggregate.MAX;
			case "sum" -> Selector.Aggregate.SUM;
			case "avg" -> Selector.Aggregate.AVG;
			default -> null;
		};
		if (aggregate == null) {
			throw new InvalidQueryException(coordinator,
				"Unknown function %s called in a SELECT clause".formatted(name));
		}
		if (args.size() != 1) {
			throw new InvalidQueryException(coordinator,
				"Invalid number of arguments in call to function %s: 1 required but found %d".formatted(
					name, args.size()));
		}

		final var argument = selector(table, args.get(0), codecRegistry, version, coordinator);
		if (argument.isAggregate()) {
			throw new InvalidQueryException(coordinator,
				"Cannot nest aggregate function %s inside %s".formatted(argument.name().asInternal(),
					name));
		}
		if ((aggregate == Selector.Aggregate.SUM || aggregate == Selector.Aggregate.AVG)
			&& !NUMERIC.contains(argument.type())) {
			throw new InvalidQueryException(coordinator,
				("Invalid call to function %s, none of its type signatures match: %s is not one of the "
					+ "numeric types it is defined over").formatted(name,
					argument.type().asCql(true, true)));
		}
		final var type = aggregate == Selector.Aggregate.COUNT ? DataTypes.BIGINT : argument.type();
		final var label = CqlIdentifier.fromInternal(
			"%s.%s(%s)".formatted(SYSTEM, name, argument.name().asInternal()));

		return aggregate(table, label, type, aggregate, argument.reader());
	}

	private static Selector aggregate(final SeaStarTable table, final CqlIdentifier name,
		final DataType type, final Selector.Aggregate aggregate, final Selector.Reader reader) {
		return new Selector(table.getKeyspace(), table.getName(), name, type, null, aggregate,
			reader);
	}

	/**
	 * {@code token(pk)}, which names the whole partition key and answers the Murmur3 token the row
	 * would live at on a cluster.
	 */
	private static Selector token(final SeaStarTable table, final List<Selectable.Raw> args,
		final CodecRegistry codecRegistry, final ProtocolVersion version, final Node coordinator) {
		final List<CqlIdentifier> partitionKey = table.getPartitionKey()
			.stream()
			.map(ColumnMetadata::getName)
			.toList();
		final List<CqlIdentifier> named = args.stream()
			.map(arg -> arg instanceof Selectable.RawIdentifier identifier
				? Selectables.toIdentifier(identifier) : null)
			.toList();
		if (named.contains(null) || !named.equals(partitionKey)) {
			throw new InvalidQueryException(coordinator,
				"The token() function must be applied to the partition key components %s".formatted(
					partitionKey.stream().map(CqlIdentifier::asInternal).toList()));
		}

		final var indices = partitionKey.stream().mapToInt(table::firstIndexOf).toArray();
		final var label = CqlIdentifier.fromInternal("%s.token(%s)".formatted(SYSTEM,
			String.join(", ", partitionKey.stream().map(CqlIdentifier::asInternal).toList())));

		return new Selector(table.getKeyspace(), table.getName(), label, DataTypes.BIGINT, null, null,
			row -> {
				final List<java.nio.ByteBuffer> components = new ArrayList<>(indices.length);
				for (final var index : indices) {
					final TypeCodec<Object> codec = codecRegistry.codecFor(table.get(index).getType());
					components.add(codec.encode(row.getObject(index), version));
				}

				return Tokens.of(Tokens.encode(components));
			});
	}

	/**
	 * {@code writetime(c)} and {@code ttl(c)}. Both read the cell's metadata rather than its value,
	 * and neither means anything for a primary key part, which is not a cell.
	 */
	private static Selector writetimeOrTtl(final SeaStarTable table,
		final Selectable.WritetimeOrTTL.Raw raw, final Node coordinator) {
		final var kind = FieldBindings.WRITETIME_KIND.require(raw);
		final var name = Selectables.toIdentifier(FieldBindings.WRITETIME_COLUMN.require(raw));
		final var index = requireColumn(table, name, coordinator);
		if (table.get(index) instanceof ColumnMetadata column && isPrimaryKey(table, column)) {
			throw new InvalidQueryException(coordinator,
				"Cannot use selection function %s on PRIMARY KEY part %s".formatted(kind.name,
					name.asInternal()));
		}
		final var label = CqlIdentifier.fromInternal(
			"%s(%s)".formatted(kind.name, name.asInternal()));
		final var ttl = kind == Selectable.WritetimeOrTTL.Kind.TTL;

		return new Selector(table.getKeyspace(), table.getName(), label,
			ttl ? DataTypes.INT : DataTypes.BIGINT, null, null,
			ttl ? row -> row.ttl(index) : row -> row.writeTime(index));
	}

	private static boolean isPrimaryKey(final SeaStarTable table, final ColumnMetadata column) {
		return table.getPartitionKey().contains(column)
			|| table.getClusteringColumns().containsKey(column);
	}

	/**
	 * {@code cast(c AS t)}. SeaStar converts between the numeric types and from anything to text,
	 * which is the whole of what a cast is normally reached for; every other pair is rejected by
	 * name rather than answered with the wrong value.
	 */
	private static Selector cast(final SeaStarTable table, final Selectable.WithCast.Raw raw,
		final CodecRegistry codecRegistry, final ProtocolVersion version, final Node coordinator) {
		final var argument = selector(table, FieldBindings.CAST_ARG.require(raw), codecRegistry,
			version, coordinator);
		final var target = SeaStarRawType.nativeDataType(FieldBindings.CAST_TYPE.require(raw))
			.orElseThrow(() -> new InvalidQueryException(coordinator,
				"SeaStar does not support a cast to %s".formatted(
					FieldBindings.CAST_TYPE.require(raw))));
		final var source = argument.type();
		final var text = DataTypes.TEXT.equals(target) || DataTypes.ASCII.equals(target);
		if (!text && !(NUMERIC.contains(source) && NUMERIC.contains(target))
			&& !source.equals(target)) {
			throw new InvalidQueryException(coordinator,
				"SeaStar does not support a cast from %s to %s".formatted(source.asCql(true, true),
					target.asCql(true, true)));
		}
		final var label = CqlIdentifier.fromInternal(
			"cast(%s as %s)".formatted(argument.name().asInternal(), target.asCql(false, false)));
		final Selector.Reader reader = row -> convert(argument.reader().read(row), source, target,
			codecRegistry);

		return new Selector(table.getKeyspace(), table.getName(), label, target, null, null, reader);
	}

	private static @Nullable Object convert(final @Nullable Object value, final DataType source,
		final DataType target, final CodecRegistry codecRegistry) {
		if (value == null || source.equals(target)) {
			return value;
		}
		if (DataTypes.TEXT.equals(target) || DataTypes.ASCII.equals(target)) {
			final TypeCodec<Object> codec = codecRegistry.codecFor(source);
			final var formatted = codec.format(value);

			// A codec formats a string-shaped value as the CQL literal it would be written as, quotes
			// and all; a cast to text is the value itself.
			return formatted.length() > 1 && formatted.charAt(0) == '\''
				? formatted.substring(1, formatted.length() - 1).replace("''", "'") : formatted;
		}

		return number((Number) value, target);
	}

	private static Number number(final Number value, final DataType target) {
		if (DataTypes.TINYINT.equals(target)) {
			return value.byteValue();
		}
		if (DataTypes.SMALLINT.equals(target)) {
			return value.shortValue();
		}
		if (DataTypes.INT.equals(target)) {
			return value.intValue();
		}
		if (DataTypes.BIGINT.equals(target) || DataTypes.COUNTER.equals(target)) {
			return value.longValue();
		}
		if (DataTypes.FLOAT.equals(target)) {
			return value.floatValue();
		}
		if (DataTypes.DOUBLE.equals(target)) {
			return value.doubleValue();
		}
		if (DataTypes.VARINT.equals(target)) {
			return BigInteger.valueOf(value.longValue());
		}

		return new BigDecimal(value.toString());
	}

}
