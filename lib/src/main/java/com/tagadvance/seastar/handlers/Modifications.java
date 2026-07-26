package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedInsert;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedUpdate;

/**
 * Translates a parsed INSERT, UPDATE or DELETE into a {@link Modification}. Together with
 * {@link Queries} this is the boundary: below it the {@code org.apache.cassandra} parse tree, above
 * it handlers that only see columns, values and operators.
 *
 * <p>Whether a column will accept the form a statement writes it in - only a list is prepended to,
 * only a counter incremented, only an unfrozen user defined type given a field - is settled here,
 * because it is a question about the column's type rather than about the row being written.
 */
final class Modifications {

	private Modifications() {
		// hidden constructor
	}

	static Modification insert(final SeaStarDriverContext context,
		final Supplier<Optional<CqlIdentifier>> sessionKeyspace, final ParsedInsert raw,
		final Node coordinator, final Object... bindings) {
		final var target = Targets.require(context, sessionKeyspace, raw, coordinator);
		final var table = target.table();
		final var codecRegistry = context.getCodecRegistry();
		final var columnNames = FieldBindings.INSERT_COLUMN_NAMES.require(raw);
		final var columnValues = FieldBindings.INSERT_COLUMN_VALUES.require(raw);

		// A counter is only ever a delta applied to what is already there, so there is no value an
		// INSERT could write.
		if (isCounterTable(target)) {
			throw new InvalidQueryException(coordinator,
				"INSERT statements are not allowed on counter table %s, use UPDATE instead".formatted(
					table.getName().asInternal()));
		}

		final List<Assignment> assignments = new ArrayList<>(columnNames.size());
		for (int i = 0; i < columnNames.size(); i++) {
			final var column = identifier(columnNames.get(i));
			final var index = columnIndex(table, column, coordinator);
			final var value = Terms.resolve(columnValues.get(i), table.get(index).getType(),
				codecRegistry, coordinator, bindings);
			assignments.add(Assignment.set(index, column, value));
		}

		return modification(target, assignments, List.of(), raw, codecRegistry, coordinator,
			bindings);
	}

	static Modification update(final SeaStarDriverContext context,
		final Supplier<Optional<CqlIdentifier>> sessionKeyspace, final ParsedUpdate raw,
		final Node coordinator, final Object... bindings) {
		final var target = Targets.require(context, sessionKeyspace, raw, coordinator);
		final var table = target.table();
		final var codecRegistry = context.getCodecRegistry();

		final var updates = FieldBindings.UPDATE_UPDATES.require(raw);
		final List<Assignment> assignments = new ArrayList<>(updates.size());
		for (final var update : updates) {
			final var column = identifier(update.left);
			final var index = columnIndex(table, column, coordinator);
			assignments.add(assignment(table, index, column, update.right, codecRegistry, coordinator,
				bindings));
		}

		final var restrictions = Restrictions.translate(table,
			FieldBindings.UPDATE_WHERE_CLAUSE.require(raw), codecRegistry, coordinator, bindings);

		return modification(target, assignments, restrictions, raw, codecRegistry, coordinator,
			bindings);
	}

	static Modification delete(final SeaStarDriverContext context,
		final Supplier<Optional<CqlIdentifier>> sessionKeyspace, final DeleteStatement.Parsed raw,
		final Node coordinator, final Object... bindings) {
		final var target = Targets.require(context, sessionKeyspace, raw, coordinator);
		final var table = target.table();
		final var codecRegistry = context.getCodecRegistry();

		// Clearing a column is an assignment of null, so DELETE name FROM t and UPDATE t SET
		// name = null describe the same write and are applied by the same code.
		final var deletions = FieldBindings.DELETE_DELETIONS.require(raw);
		final List<Assignment> assignments = new ArrayList<>(deletions.size());
		for (final var deletion : deletions) {
			assignments.add(deletion(table, deletion, codecRegistry, coordinator, bindings));
		}

		final var restrictions = Restrictions.translate(table,
			FieldBindings.DELETE_WHERE_CLAUSE.require(raw), codecRegistry, coordinator, bindings);

		return modification(target, assignments, restrictions, raw, codecRegistry, coordinator,
			bindings);
	}

	private static Modification modification(final Target target,
		final List<Assignment> assignments, final List<Restriction> restrictions,
		final ModificationStatement.Parsed raw, final CodecRegistry codecRegistry,
		final Node coordinator, final Object... bindings) {
		final var conditions = Conditions.translate(target, raw.getConditions(), codecRegistry,
			coordinator, bindings);
		// A counter has no value to compare against: what it reads back depends on every delta that
		// reached it, so Cassandra will not condition on one.
		final var table = target.table();
		conditions.stream()
			.filter(condition -> DataTypes.COUNTER.equals(table.get(condition.columnIndex()).getType()))
			.findFirst()
			.ifPresent(condition -> {
				throw new InvalidQueryException(coordinator,
					"Conditions on counter column %s are not supported".formatted(
						condition.column().asInternal()));
			});

		return new Modification(target, List.copyOf(assignments), restrictions, conditions,
			FieldBindings.MODIFICATION_IF_EXISTS.require(raw),
			FieldBindings.MODIFICATION_IF_NOT_EXISTS.require(raw));
	}

	/**
	 * One {@code SET} item of an UPDATE, checked against the type of the column it writes.
	 */
	private static Assignment assignment(final SeaStarTable table, final int index,
		final CqlIdentifier column, final Operation.RawUpdate raw, final CodecRegistry codecRegistry,
		final Node coordinator, final Object... bindings) {
		final var type = table.get(index).getType();
		if (raw instanceof Operation.SetValue setValue) {
			if (DataTypes.COUNTER.equals(type)) {
				throw new InvalidQueryException(coordinator,
					("Cannot set the value of counter column %s (counters can only be "
						+ "incremented/decremented, not set)").formatted(column.asInternal()));
			}

			return Assignment.set(index, column,
				Terms.resolve(FieldBindings.SET_VALUE.require(setValue), type, codecRegistry,
					coordinator, bindings));
		}
		if (raw instanceof Operation.Addition addition) {
			return accumulate(index, column, type, FieldBindings.ADDITION_VALUE.require(addition),
				Assignment.Operator.APPEND, Assignment.Operator.INCREMENT, "+", codecRegistry,
				coordinator, bindings);
		}
		if (raw instanceof Operation.Substraction subtraction) {
			return accumulate(index, column, type,
				FieldBindings.SUBTRACTION_VALUE.require(subtraction), Assignment.Operator.DISCARD,
				Assignment.Operator.DECREMENT, "-", codecRegistry, coordinator, bindings);
		}
		if (raw instanceof Operation.Prepend prepend) {
			return prepend(index, column, type, FieldBindings.PREPEND_VALUE.require(prepend),
				codecRegistry, coordinator, bindings);
		}
		if (raw instanceof Operation.SetElement setElement) {
			return setElement(index, column, type,
				FieldBindings.SET_ELEMENT_SELECTOR.require(setElement),
				FieldBindings.SET_ELEMENT_VALUE.require(setElement), codecRegistry, coordinator,
				bindings);
		}
		if (raw instanceof Operation.SetField setField) {
			return setField(index, column, type, FieldBindings.SET_FIELD_FIELD.require(setField),
				FieldBindings.SET_FIELD_VALUE.require(setField), codecRegistry, coordinator, bindings);
		}

		throw new UnsupportedOperationException("Unsupported UPDATE assignment %s".formatted(raw));
	}

	/**
	 * {@code c = c + v} and {@code c = c - v}, which a collection reads as adding or removing
	 * elements and a counter as a delta. Removing from a map names keys rather than entries, so its
	 * term is a set of the map's key type.
	 */
	private static Assignment accumulate(final int index, final CqlIdentifier column,
		final DataType type, final Term.Raw term, final Assignment.Operator collection,
		final Assignment.Operator counter, final String symbol, final CodecRegistry codecRegistry,
		final Node coordinator, final Object... bindings) {
		if (DataTypes.COUNTER.equals(type)) {
			return new Assignment(index, column, counter, null,
				Terms.resolve(term, DataTypes.COUNTER, codecRegistry, coordinator, bindings));
		}
		final var termType = type instanceof MapType map
			&& collection == Assignment.Operator.DISCARD ? DataTypes.setOf(map.getKeyType()) : type;
		if (!isCollection(type)) {
			throw new InvalidQueryException(coordinator,
				"Invalid operation (%s = %s %s %s) for non counter column %s".formatted(
					column.asInternal(), column.asInternal(), symbol, term.getText(),
					column.asInternal()));
		}

		return new Assignment(index, column, collection, null,
			Terms.resolve(term, termType, codecRegistry, coordinator, bindings));
	}

	private static Assignment prepend(final int index, final CqlIdentifier column,
		final DataType type, final Term.Raw term, final CodecRegistry codecRegistry,
		final Node coordinator, final Object... bindings) {
		if (!(type instanceof ListType)) {
			throw new InvalidQueryException(coordinator,
				"Invalid operation (%s = %s + %s) for non list column %s".formatted(
					column.asInternal(), term.getText(), column.asInternal(), column.asInternal()));
		}

		return new Assignment(index, column, Assignment.Operator.PREPEND, null,
			Terms.resolve(term, type, codecRegistry, coordinator, bindings));
	}

	/**
	 * {@code c[k] = v}: an index into a list or a key into a map. A frozen collection is one value
	 * rather than one cell per element, so no element of it can be written on its own.
	 */
	private static Assignment setElement(final int index, final CqlIdentifier column,
		final DataType type, final Term.Raw selector, final Term.Raw term,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		final var written = "%s[%s] = %s".formatted(column.asInternal(), selector.getText(),
			term.getText());
		if (type instanceof ListType list) {
			requireMultiCell(list.isFrozen(), column, written, coordinator);

			return new Assignment(index, column, Assignment.Operator.SET_LIST_ELEMENT,
				Terms.resolve(selector, DataTypes.INT, codecRegistry, coordinator, bindings),
				Terms.resolve(term, list.getElementType(), codecRegistry, coordinator, bindings));
		}
		if (type instanceof MapType map) {
			requireMultiCell(map.isFrozen(), column, written, coordinator);

			return new Assignment(index, column, Assignment.Operator.SET_MAP_ENTRY,
				Terms.resolve(selector, map.getKeyType(), codecRegistry, coordinator, bindings),
				Terms.resolve(term, map.getValueType(), codecRegistry, coordinator, bindings));
		}
		if (type instanceof SetType) {
			throw new InvalidQueryException(coordinator,
				"Invalid operation (%s) for set column %s".formatted(written, column.asInternal()));
		}

		throw new InvalidQueryException(coordinator,
			"Invalid operation (%s) for non collection column %s".formatted(written,
				column.asInternal()));
	}

	private static Assignment setField(final int index, final CqlIdentifier column,
		final DataType type, final FieldIdentifier field, final Term.Raw term,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		final var name = CqlIdentifier.fromInternal(field.toString());
		final var written = "%s.%s = %s".formatted(column.asInternal(), name.asInternal(),
			term.getText());
		if (!(type instanceof UserDefinedType udt)) {
			throw new InvalidQueryException(coordinator,
				"Invalid operation (%s) for non-UDT column %s".formatted(written,
					column.asInternal()));
		}
		if (udt.isFrozen()) {
			throw new InvalidQueryException(coordinator,
				"Invalid operation (%s) for frozen UDT column %s".formatted(written,
					column.asInternal()));
		}
		final var fieldIndex = requireField(udt, column, name, coordinator);

		return new Assignment(index, column, Assignment.Operator.SET_FIELD, fieldIndex,
			Terms.resolve(term, udt.getFieldTypes().get(fieldIndex), codecRegistry, coordinator,
				bindings));
	}

	/**
	 * One item of a DELETE's column list: a whole column, one element of a collection, or one field
	 * of a user defined type. {@code DELETE m['k']} removes that entry alone - reading it as the
	 * whole column would silently throw the rest of the map away.
	 */
	private static Assignment deletion(final SeaStarTable table, final Operation.RawDeletion raw,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		final var column = identifier(raw.affectedColumn());
		final var index = columnIndex(table, column, coordinator);
		final var type = table.get(index).getType();

		if (raw instanceof Operation.ElementDeletion elementDeletion) {
			final var element = FieldBindings.ELEMENT_DELETION_ELEMENT.require(elementDeletion);
			final var written = "%s[%s]".formatted(column.asInternal(), element.getText());
			if (type instanceof ListType list) {
				requireMultiCell(list.isFrozen(), column, written, coordinator);

				return new Assignment(index, column, Assignment.Operator.DELETE_LIST_ELEMENT,
					Terms.resolve(element, DataTypes.INT, codecRegistry, coordinator, bindings), null);
			}
			if (type instanceof SetType set) {
				requireMultiCell(set.isFrozen(), column, written, coordinator);

				return new Assignment(index, column, Assignment.Operator.DELETE_SET_ELEMENT,
					Terms.resolve(element, set.getElementType(), codecRegistry, coordinator, bindings),
					null);
			}
			if (type instanceof MapType map) {
				requireMultiCell(map.isFrozen(), column, written, coordinator);

				return new Assignment(index, column, Assignment.Operator.DELETE_MAP_ENTRY,
					Terms.resolve(element, map.getKeyType(), codecRegistry, coordinator, bindings), null);
			}

			throw new InvalidQueryException(coordinator,
				"Invalid deletion operation for non collection column %s".formatted(
					column.asInternal()));
		}

		if (raw instanceof Operation.FieldDeletion fieldDeletion) {
			final var name = CqlIdentifier.fromInternal(
				FieldBindings.FIELD_DELETION_FIELD.require(fieldDeletion).toString());
			if (!(type instanceof UserDefinedType udt)) {
				throw new InvalidQueryException(coordinator,
					"Invalid field deletion operation for non-UDT column %s".formatted(
						column.asInternal()));
			}
			if (udt.isFrozen()) {
				throw new InvalidQueryException(coordinator,
					"Frozen UDT column %s does not support field deletions".formatted(
						column.asInternal()));
			}

			return new Assignment(index, column, Assignment.Operator.DELETE_FIELD,
				requireField(udt, column, name, coordinator), null);
		}

		return Assignment.set(index, column, null);
	}

	private static void requireMultiCell(final boolean frozen, final CqlIdentifier column,
		final String written, final Node coordinator) {
		if (frozen) {
			throw new InvalidQueryException(coordinator,
				"Invalid operation (%s) for frozen collection column %s".formatted(written,
					column.asInternal()));
		}
	}

	private static int requireField(final UserDefinedType udt, final CqlIdentifier column,
		final CqlIdentifier field, final Node coordinator) {
		final var fieldIndex = udt.firstIndexOf(field);
		if (fieldIndex < 0) {
			throw new InvalidQueryException(coordinator,
				"UDT column %s does not have a field named %s".formatted(column.asInternal(),
					field.asInternal()));
		}

		return fieldIndex;
	}

	private static boolean isCollection(final DataType type) {
		return type instanceof ListType || type instanceof SetType || type instanceof MapType;
	}

	/**
	 * A counter table is one whose non-key columns are counters. Cassandra will not let the two
	 * kinds share a table, so any one of them settles it.
	 */
	private static boolean isCounterTable(final Target target) {
		final var primaryKey = target.primaryKeyNames();

		return target.table()
			.getColumns()
			.values()
			.stream()
			.filter(column -> !primaryKey.contains(column.getName()))
			.anyMatch(column -> DataTypes.COUNTER.equals(column.getType()));
	}

	private static CqlIdentifier identifier(final ColumnIdentifier name) {
		return CqlIdentifier.fromInternal(name.toString());
	}

	private static int columnIndex(final SeaStarTable table, final CqlIdentifier column,
		final Node coordinator) {
		final var index = table.firstIndexOf(column);
		if (index < 0) {
			throw new InvalidQueryException(coordinator,
				"Undefined column name %s".formatted(column.asInternal()));
		}

		return index;
	}

}
