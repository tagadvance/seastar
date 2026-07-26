package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedInsert;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedUpdate;

/**
 * Translates a parsed INSERT, UPDATE or DELETE into a {@link Modification}. Together with
 * {@link Queries} this is the boundary: below it the {@code org.apache.cassandra} parse tree, above
 * it handlers that only see columns, values and operators.
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

		final List<Assignment> assignments = new ArrayList<>(columnNames.size());
		for (int i = 0; i < columnNames.size(); i++) {
			final var column = identifier(columnNames.get(i));
			final var index = columnIndex(table, column, coordinator);
			final var value = Terms.resolve(columnValues.get(i), table.get(index).getType(),
				codecRegistry, coordinator, bindings);
			assignments.add(new Assignment(index, column, value));
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
			// Only SET <column> = <term> is described; +=, [k] = and .f = belong here when the model
			// grows to hold them.
			if (!(update.right instanceof Operation.SetValue setValue)) {
				throw new UnsupportedOperationException(
					"Unsupported UPDATE assignment %s".formatted(update.right));
			}
			final var value = Terms.resolve(FieldBindings.SET_VALUE.require(setValue),
				table.get(index).getType(), codecRegistry, coordinator, bindings);
			assignments.add(new Assignment(index, column, value));
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
			final var column = identifier(deletion.affectedColumn());
			assignments.add(new Assignment(columnIndex(table, column, coordinator), column, null));
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

		return new Modification(target, List.copyOf(assignments), restrictions, conditions,
			FieldBindings.MODIFICATION_IF_EXISTS.require(raw),
			FieldBindings.MODIFICATION_IF_NOT_EXISTS.require(raw));
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
