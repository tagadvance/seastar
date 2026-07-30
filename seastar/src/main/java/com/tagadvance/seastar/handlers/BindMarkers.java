package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.MultiColumnRelation;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.QualifiedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedInsert;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedUpdate;
import org.apache.cassandra.utils.Pair;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Resolves the bind-marker (variable) and result-set {@link ColumnDefinitions} for a parsed
 * statement, by walking the {@link CQLStatement.Raw} parse tree and matching bind markers to the
 * columns of the target table. This is what {@code SeaStarPreparedStatement} exposes as
 * {@code getVariableDefinitions()} / {@code getResultSetDefinitions()}.
 *
 * <p>Resolution is what makes {@code prepare()} validate: a live cluster rejects a statement naming
 * a keyspace, table or column that does not exist at prepare time, so this throws rather than
 * answering with empty definitions. A statement that addresses no table - DDL, TRUNCATE - carries no
 * markers and resolves to nothing, which is why preparing one succeeds.
 *
 * <p>{@link #values(SeaStarDriverContext, CqlIdentifier, CQLStatement.Raw, SimpleStatement)} is the
 * other side of the same question: which of a statement's own values goes to which marker. It is
 * here because the parse tree is the only place the markers are counted and named.
 */
public final class BindMarkers {

	public record Definitions(ColumnDefinitions variables, ColumnDefinitions resultSet,
		List<Integer> partitionKeyIndices) {

	}

	private static final Definitions EMPTY = new Definitions(EmptyColumnDefinitions.INSTANCE,
		EmptyColumnDefinitions.INSTANCE, List.of());

	private static final Object[] NO_VALUES = {};

	private static final String WRONG_COUNT = "Invalid amount of bind variables";

	private BindMarkers() {
	}

	/**
	 * The values a {@link SimpleStatement} supplies for its bind markers, by bind index, which is what
	 * the translation layer resolves a marker against.
	 *
	 * <p>A statement carries positional values or named ones, never both - the driver's builder
	 * refuses the mixture - so those are the two cases here. Positional values line up with the
	 * markers as written; named ones are matched to the name a {@code :name} marker was written with,
	 * or, for a {@code ?}, to the column it stands for, which is how a node resolves them.
	 *
	 * <p>The two are counted differently, and deliberately. A node checks positional values against
	 * the marker count, but resolves named ones with one lookup per marker
	 * ({@code QueryOptions.OptionsWithNames}): a marker no name accounts for is the count complaint,
	 * a name no marker claims is ignored, and one value feeds every marker sharing its name.
	 *
	 * @throws InvalidQueryException if the values do not account for every marker the statement
	 *                               carries, which is what a node answers rather than binding null
	 */
	public static Object[] values(final SeaStarDriverContext context,
		final @Nullable CqlIdentifier sessionKeyspace, final CQLStatement.Raw raw,
		final SimpleStatement statement) {
		final var coordinator = context.getNode();
		final var markers = FieldBindings.BIND_VARIABLE_NAMES.require(
			FieldBindings.STATEMENT_BIND_VARIABLES.require(raw));
		final var positional = statement.getPositionalValues();
		final var named = statement.getNamedValues();
		if (positional.isEmpty() && named.isEmpty()) {
			// Unlike a prepared statement, which may leave its trailing markers unbound, a statement
			// executed straight off a string has to account for every one of them.
			if (!markers.isEmpty()) {
				throw new InvalidQueryException(coordinator, WRONG_COUNT);
			}

			return NO_VALUES;
		}

		if (!positional.isEmpty()) {
			if (positional.size() != markers.size()) {
				throw new InvalidQueryException(coordinator, WRONG_COUNT);
			}

			return positional.toArray();
		}

		// A ? carries no name of its own, so the columns are only resolved when one has to be named.
		final var columns = markers.contains(null)
			? resolve(context, sessionKeyspace, raw).variables() : EmptyColumnDefinitions.INSTANCE;
		final var values = new Object[markers.size()];
		for (int i = 0; i < markers.size(); i++) {
			final var name = name(markers.get(i), columns, i);
			if (name == null || !named.containsKey(name)) {
				throw new InvalidQueryException(coordinator, WRONG_COUNT);
			}
			values[i] = named.get(name);
		}

		return values;
	}

	/**
	 * The name a named value is matched against for one marker: the one it was written with, else the
	 * column it stands for, else null where SeaStar could not map the marker to a column.
	 */
	private static @Nullable CqlIdentifier name(final @Nullable ColumnIdentifier marker,
		final ColumnDefinitions columns, final int index) {
		if (marker != null) {
			return CqlIdentifier.fromInternal(marker.toString());
		}

		return index < columns.size() ? columns.get(index).getName() : null;
	}

	/**
	 * Resolves the bind markers a statement carries.
	 *
	 * <p>A live cluster validates at prepare time, so a statement naming a keyspace, table or column
	 * that does not exist fails here rather than at bind or execute. Statements that address no table
	 * at all - DDL, TRUNCATE - carry no markers and resolve to nothing, which is also what a cluster
	 * does: preparing them succeeds.
	 *
	 * <p>A variable is named after the marker rather than after the column it binds - see
	 * {@link #toDefinitions}. The column is what types it, and what
	 * {@link Definitions#partitionKeyIndices()} is computed from.
	 *
	 * @throws InvalidQueryException if the statement addresses something that does not exist
	 */
	public static Definitions resolve(final SeaStarDriverContext context,
		final CqlIdentifier sessionKeyspace, final CQLStatement.Raw raw) {
		if (!(raw instanceof QualifiedStatement qualified)) {
			return EMPTY;
		}

		final var coordinator = context.getNode();
		// keyspace() throws rather than returning null when the statement was never qualified.
		final var keyspace = Optional.of(qualified)
			.filter(QualifiedStatement::isFullyQualified)
			.map(QualifiedStatement::keyspace)
			.map(CqlIdentifier::fromInternal)
			.or(() -> Optional.ofNullable(sessionKeyspace))
			.orElseThrow(() -> new InvalidQueryException(coordinator,
				"No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename"));

		final var table = context.getSeaStarKeyspace(keyspace)
			.orElseThrow(() -> new InvalidQueryException(coordinator,
				"keyspace %s does not exist".formatted(keyspace.asInternal())))
			.getSeaStarTable(CqlIdentifier.fromInternal(qualified.name()))
			.orElseThrow(() -> new InvalidQueryException(coordinator,
				"table %s does not exist".formatted(qualified.name())));

		final NavigableMap<Integer, ColumnDefinition> markers = new TreeMap<>();
		final ColumnDefinitions resultSet;
		if (raw instanceof ParsedInsert insert) {
			collectInsert(table, insert, markers, coordinator);
			collectModification(table, insert, markers, coordinator);
			resultSet = EmptyColumnDefinitions.INSTANCE;
		} else if (raw instanceof ParsedUpdate update) {
			collectUpdate(table, update, markers, coordinator);
			collectModification(table, update, markers, coordinator);
			resultSet = EmptyColumnDefinitions.INSTANCE;
		} else if (raw instanceof DeleteStatement.Parsed delete) {
			collectWhere(table, FieldBindings.DELETE_WHERE_CLAUSE.require(delete).relations, markers,
				coordinator);
			collectModification(table, delete, markers, coordinator);
			resultSet = EmptyColumnDefinitions.INSTANCE;
		} else if (raw instanceof SelectStatement.RawStatement select) {
			collectSelect(table, select, markers, coordinator);
			resultSet = resolveSelectResult(table, select.selectClause, coordinator);
		} else {
			return EMPTY;
		}

		final var variables = toDefinitions(table, markers,
			FieldBindings.BIND_VARIABLE_NAMES.require(
				FieldBindings.STATEMENT_BIND_VARIABLES.require(raw)));
		// A marker we could not map leaves toDefinitions empty, so indices into it would be meaningless.
		final var partitionKeyIndices =
			variables.size() == markers.size() ? partitionKeyIndices(table, markers) : List.<Integer>of();

		return new Definitions(variables, resultSet, partitionKeyIndices);
	}

	/**
	 * Mirrors {@code VariableSpecifications.getPartitionKeyBindVariableIndexes}: the result is
	 * ordered by partition key position and holds the bind index of the marker supplying that
	 * component, or is empty unless every component is supplied by a marker. Where a component is
	 * supplied more than once (e.g. {@code pk IN (?, ?)}) the highest bind index wins, as it does in
	 * Cassandra.
	 */
	private static List<Integer> partitionKeyIndices(final SeaStarTable table,
		final NavigableMap<Integer, ColumnDefinition> markers) {
		final Map<CqlIdentifier, Integer> byColumn = markers.entrySet()
			.stream()
			.collect(Collectors.toMap(entry -> entry.getValue().getName(), Map.Entry::getKey,
				(lower, higher) -> higher));

		final var partitionKey = table.getPartitionKey();
		final var indices = partitionKey.stream()
			.map(ColumnMetadata::getName)
			.map(byColumn::get)
			.filter(Objects::nonNull)
			.toList();

		return indices.size() == partitionKey.size() ? indices : List.of();
	}

	private static void collectInsert(final SeaStarTable table, final ParsedInsert raw,
		final NavigableMap<Integer, ColumnDefinition> markers, final Node coordinator) {
		final var columnNames = FieldBindings.INSERT_COLUMN_NAMES.require(raw);
		final var columnValues = FieldBindings.INSERT_COLUMN_VALUES.require(raw);
		for (int i = 0; i < columnNames.size(); i++) {
			final var column = requireColumn(table, columnNames.get(i).toString(), coordinator);
			collectInsertValue(table, columnValues.get(i), column, markers);
		}
	}

	/**
	 * Registers the markers in one INSERT value, descending into UDT literals so that a marker
	 * standing in for a field is typed as that field rather than as the whole UDT.
	 */
	private static void collectInsertValue(final SeaStarTable table, final Term.Raw term,
		final ColumnDefinition column, final NavigableMap<Integer, ColumnDefinition> markers) {
		if (term instanceof UserTypes.Literal literal
			&& column.getType() instanceof UserDefinedType udt) {
			// Cassandra names a field variable "column.field"; see UserTypes.Literal.fieldSpecOf.
			literal.entries.forEach((field, fieldTerm) -> {
				final var name = CqlIdentifier.fromInternal(field.toString());
				final var index = udt.firstIndexOf(name);
				if (index >= 0) {
					final var definition = syntheticDefinition(table,
						"%s.%s".formatted(column.getName().asInternal(), name.asInternal()),
						udt.getFieldTypes().get(index));
					collectInsertValue(table, fieldTerm, definition, markers);
				}
			});
		} else {
			putIfMarker(term, column, markers);
		}
	}

	private static void collectUpdate(final SeaStarTable table, final ParsedUpdate raw,
		final NavigableMap<Integer, ColumnDefinition> markers, final Node coordinator) {
		for (final var update : FieldBindings.UPDATE_UPDATES.require(raw)) {
			final var column = requireColumn(table, update.left.toString(), coordinator);
			updateTerms(update.right).forEach(term -> putIfMarker(term, column, markers));
		}
		collectWhere(table, FieldBindings.UPDATE_WHERE_CLAUSE.require(raw).relations, markers,
			coordinator);
	}

	/**
	 * The terms one {@code SET} item is written with. A marker inside a collection or element form
	 * is typed as the whole column, which is coarser than a cluster types it, but leaving it out
	 * would put a gap in the bind indices and cost the statement its variable definitions entirely.
	 */
	private static List<Term.Raw> updateTerms(final Operation.RawUpdate raw) {
		if (raw instanceof Operation.SetValue setValue) {
			return List.of(FieldBindings.SET_VALUE.require(setValue));
		}
		if (raw instanceof Operation.Addition addition) {
			return List.of(FieldBindings.ADDITION_VALUE.require(addition));
		}
		if (raw instanceof Operation.Substraction subtraction) {
			return List.of(FieldBindings.SUBTRACTION_VALUE.require(subtraction));
		}
		if (raw instanceof Operation.Prepend prepend) {
			return List.of(FieldBindings.PREPEND_VALUE.require(prepend));
		}
		if (raw instanceof Operation.SetElement setElement) {
			return List.of(FieldBindings.SET_ELEMENT_SELECTOR.require(setElement),
				FieldBindings.SET_ELEMENT_VALUE.require(setElement));
		}
		if (raw instanceof Operation.SetField setField) {
			return List.of(FieldBindings.SET_FIELD_VALUE.require(setField));
		}

		return List.of();
	}

	/**
	 * The markers an INSERT, UPDATE or DELETE carries outside its columns: the {@code USING} clause
	 * and the {@code IF} conditions. Neither is typed by a column - a TTL is an {@code int} named
	 * {@code [ttl]} and a timestamp a {@code bigint} named {@code [timestamp]}, which is what
	 * {@code Attributes.Raw} calls its two receivers - and both are ordinary bind markers a node
	 * reports, so leaving them out left the metadata short.
	 */
	private static void collectModification(final SeaStarTable table,
		final ModificationStatement.Parsed raw,
		final NavigableMap<Integer, ColumnDefinition> markers, final Node coordinator) {
		final var attributes = FieldBindings.MODIFICATION_ATTRIBUTES.require(raw);
		if (attributes.timeToLive != null) {
			putIfMarker(attributes.timeToLive, syntheticDefinition(table, "[ttl]", DataTypes.INT),
				markers);
		}
		if (attributes.timestamp != null) {
			putIfMarker(attributes.timestamp,
				syntheticDefinition(table, "[timestamp]", DataTypes.BIGINT), markers);
		}
		collectConditions(table, raw.getConditions(), markers, coordinator);
	}

	/**
	 * The markers of an {@code IF} clause, each typed as the column it compares. A marker the clause
	 * carries somewhere this does not reach - {@code IF v IN ?} binds the whole list to one - is left
	 * out, and the total count in {@link #toDefinitions} turns that into empty metadata rather than
	 * into short metadata.
	 */
	private static void collectConditions(final SeaStarTable table,
		final List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions,
		final NavigableMap<Integer, ColumnDefinition> markers, final Node coordinator) {
		for (final var condition : conditions) {
			final var column = requireColumn(table, condition.left.toString(), coordinator);
			final var value = condition.right.getValue();
			if (value != null) {
				putIfMarker(value, column, markers);
			}
			FieldBindings.CONDITION_IN_VALUES.find(condition.right)
				.stream()
				.flatMap(List::stream)
				.forEach(term -> putIfMarker(term, column, markers));
		}
	}

	private static void collectSelect(final SeaStarTable table,
		final SelectStatement.RawStatement raw,
		final NavigableMap<Integer, ColumnDefinition> markers, final Node coordinator) {
		if (raw.whereClause != null) {
			collectWhere(table, raw.whereClause.relations, markers, coordinator);
		}
		if (raw.limit instanceof AbstractMarker.Raw) {
			putIfMarker(raw.limit, syntheticDefinition(table, "[limit]", DataTypes.INT), markers);
		}
	}

	private static void collectWhere(final SeaStarTable table, final List<Relation> relations,
		final NavigableMap<Integer, ColumnDefinition> markers, final Node coordinator) {
		for (final var relation : relations) {
			if (relation instanceof MultiColumnRelation multi) {
				// A multi-column relation compares a tuple of clustering columns, so its markers sit
				// inside a tuple literal rather than beside the column they stand for. A node types each
				// one as its own column all the same.
				final var columns = multi.getEntities()
					.stream()
					.map(entity -> requireColumn(table, entity.toString(), coordinator))
					.toList();
				// Both accessors assert on the operator, so which one is readable is decided here rather
				// than by a null check.
				if (!multi.isIN()) {
					collectTuple(multi.getValue(), columns, markers);
				} else if (multi.getInValues() != null) {
					// Null is `(a, b) IN ?`, one marker for the whole list, which SeaStar does not model.
					multi.getInValues().forEach(term -> collectTuple(term, columns, markers));
				}

				continue;
			}
			if (!(relation instanceof SingleColumnRelation single)) {
				continue;
			}
			final var column = requireColumn(table, single.getEntity().toString(), coordinator);
			if (single.getValue() != null) {
				putIfMarker(single.getValue(), column, markers);
			}
			if (single.getInValues() != null) {
				for (final var term : single.getInValues()) {
					putIfMarker(term, column, markers);
				}
			}
		}
	}

	/**
	 * The markers of one multi-column relation value, positionally matched to the columns the
	 * relation names. {@code (a, b) = ?} binds the whole tuple to a single marker instead, which
	 * SeaStar does not model and so does not register.
	 */
	private static void collectTuple(final Term.Raw term, final List<ColumnDefinition> columns,
		final NavigableMap<Integer, ColumnDefinition> markers) {
		if (!(term instanceof Tuples.Literal literal)) {
			return;
		}
		final var elements = FieldBindings.TUPLE_ELEMENTS.require(literal);
		for (int i = 0; i < Math.min(elements.size(), columns.size()); i++) {
			putIfMarker(elements.get(i), columns.get(i), markers);
		}
	}

	private static ColumnDefinitions resolveSelectResult(final SeaStarTable table,
		final List<RawSelector> selectClause, final Node coordinator) {
		if (selectClause.isEmpty()) {
			return table.snapshot();
		}

		final List<ColumnDefinition> columns = new ArrayList<>(selectClause.size());
		for (final var selector : selectClause) {
			if (!(selector.selectable instanceof Selectable.RawIdentifier identifier)) {
				return EmptyColumnDefinitions.INSTANCE;
			}
			final var name = Selectables.toIdentifier(identifier);
			final var index = table.firstIndexOf(name);
			if (index < 0) {
				throw new InvalidQueryException(coordinator,
					"Undefined column name %s in table %s.%s".formatted(name.asInternal(),
						table.getKeyspace().asInternal(), table.getName().asInternal()));
			}
			columns.add(table.get(index));
		}

		return DefaultColumnDefinitions.valueOf(columns);
	}

	/**
	 * A live cluster rejects a prepare that names a column the table does not have, rather than
	 * deferring the failure to bind or execute.
	 */
	private static ColumnDefinition requireColumn(final SeaStarTable table, final String name,
		final Node coordinator) {
		final var index = table.firstIndexOf(CqlIdentifier.fromInternal(name));
		if (index < 0) {
			throw new InvalidQueryException(coordinator,
				"Undefined column name %s in table %s.%s".formatted(name,
					table.getKeyspace().asInternal(), table.getName().asInternal()));
		}

		return table.get(index);
	}

	private static void putIfMarker(final Term.Raw term, final ColumnDefinition column,
		final NavigableMap<Integer, ColumnDefinition> markers) {
		if (term instanceof AbstractMarker.Raw marker) {
			markers.put(FieldBindings.MARKER_BIND_INDEX.require(marker), column);
		}
	}

	/**
	 * The variable definitions for the markers collected, named the way a node names them: after the
	 * name a {@code :name} marker was written with, else after the column a {@code ?} stands for. The
	 * keyspace, table and type stay the column's either way, which is what
	 * {@code VariableSpecifications#add} does - it rebuilds the spec around the written name alone.
	 *
	 * <p>{@code names} is one entry per marker in bind index order, null where the marker was
	 * anonymous. Nothing is deduplicated: {@code :x} written twice is two definitions both named
	 * {@code x}, as it is on a node.
	 */
	private static ColumnDefinitions toDefinitions(final SeaStarTable table,
		final NavigableMap<Integer, ColumnDefinition> markers,
		final List<ColumnIdentifier> names) {
		// names has one entry per marker the parser saw, so a shorter map is a marker we could not map
		// and the metadata cannot be reliably indexed by position. Counting rather than only looking
		// for a gap is what catches an uncollected marker that happens to be the last one, where there
		// is no gap to find and the metadata would otherwise come out silently short.
		if (markers.isEmpty() || markers.size() != names.size()) {
			return EmptyColumnDefinitions.INSTANCE;
		}
		// Bind indices are assigned sequentially by the parser, so a full map runs 0..size-1.
		if (markers.firstKey() != 0 || markers.lastKey() != markers.size() - 1) {
			return EmptyColumnDefinitions.INSTANCE;
		}

		final List<ColumnDefinition> definitions = new ArrayList<>(markers.size());
		markers.forEach((index, column) -> {
			final var written = index < names.size() ? names.get(index) : null;
			definitions.add(written == null ? column
				: syntheticDefinition(table, written.toString(), column.getType()));
		});

		return DefaultColumnDefinitions.valueOf(definitions);
	}

	private static ColumnDefinition syntheticDefinition(final SeaStarTable table, final String name,
		final DataType type) {
		final var keyspace = table.getKeyspace();
		final var tableName = table.getName();
		final var columnName = CqlIdentifier.fromInternal(name);

		return new ColumnDefinition() {

			@Override
			@NonNull
			public CqlIdentifier getKeyspace() {
				return keyspace;
			}

			@Override
			@NonNull
			public CqlIdentifier getTable() {
				return tableName;
			}

			@Override
			@NonNull
			public CqlIdentifier getName() {
				return columnName;
			}

			@Override
			@NonNull
			public DataType getType() {
				return type;
			}

			@Override
			public boolean isDetached() {
				return false;
			}

			@Override
			public void attach(final @NonNull AttachmentPoint attachmentPoint) {
			}

		};
	}

}
