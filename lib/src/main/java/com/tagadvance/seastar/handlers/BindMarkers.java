package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
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
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.QualifiedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedInsert;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedUpdate;
import org.apache.cassandra.utils.Pair;
import org.jspecify.annotations.NonNull;

/**
 * Resolves the bind-marker (variable) and result-set {@link ColumnDefinitions} for a parsed
 * statement, by walking the {@link CQLStatement.Raw} parse tree and matching bind markers to the
 * columns of the target table. This is what {@code SeaStarPreparedStatement} exposes as
 * {@code getVariableDefinitions()} / {@code getResultSetDefinitions()}.
 *
 * <p>Best effort: if the keyspace/table cannot be resolved (or the statement type is unsupported),
 * empty definitions are returned rather than throwing, since metadata inspection must not fail
 * during prepare.
 */
public final class BindMarkers {

	public record Definitions(ColumnDefinitions variables, ColumnDefinitions resultSet,
		List<Integer> partitionKeyIndices) {

	}

	private static final Definitions EMPTY = new Definitions(EmptyColumnDefinitions.INSTANCE,
		EmptyColumnDefinitions.INSTANCE, List.of());

	private BindMarkers() {
	}

	public static Definitions resolve(final SeaStarDriverContext context,
		final CqlIdentifier sessionKeyspace, final CQLStatement.Raw raw) {
		try {
			return resolveInternal(context, sessionKeyspace, raw);
		} catch (final RuntimeException e) {
			return EMPTY;
		}
	}

	private static Definitions resolveInternal(final SeaStarDriverContext context,
		final CqlIdentifier sessionKeyspace, final CQLStatement.Raw raw) {
		if (!(raw instanceof QualifiedStatement qualified)) {
			return EMPTY;
		}

		final var keyspace = Optional.ofNullable(qualified.keyspace())
			.map(CqlIdentifier::fromInternal)
			.or(() -> Optional.ofNullable(sessionKeyspace))
			.orElse(null);
		if (keyspace == null) {
			return EMPTY;
		}

		final var table = context.getSeaStarKeyspace(keyspace)
			.flatMap(ks -> ks.getSeaStarTable(CqlIdentifier.fromInternal(qualified.name())))
			.orElse(null);
		if (table == null) {
			return EMPTY;
		}

		final NavigableMap<Integer, ColumnDefinition> markers = new TreeMap<>();
		final ColumnDefinitions resultSet;
		if (raw instanceof ParsedInsert insert) {
			collectInsert(table, insert, markers);
			resultSet = EmptyColumnDefinitions.INSTANCE;
		} else if (raw instanceof ParsedUpdate update) {
			collectUpdate(table, update, markers);
			resultSet = EmptyColumnDefinitions.INSTANCE;
		} else if (raw instanceof DeleteStatement.Parsed delete) {
			collectWhere(table, whereRelations(delete), markers);
			resultSet = EmptyColumnDefinitions.INSTANCE;
		} else if (raw instanceof SelectStatement.RawStatement select) {
			collectSelect(table, select, markers);
			resultSet = resolveSelectResult(table, select.selectClause);
		} else {
			return EMPTY;
		}

		final var variables = toDefinitions(markers);
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

	@SuppressWarnings("unchecked")
	private static void collectInsert(final SeaStarTable table, final ParsedInsert raw,
		final NavigableMap<Integer, ColumnDefinition> markers) {
		final List<Object> columnNames = Reflections.getDeclaredField(raw, "columnNames",
			List.class).orElseThrow();
		final List<Object> columnValues = Reflections.getDeclaredField(raw, "columnValues",
			List.class).orElseThrow();
		for (int i = 0; i < columnNames.size(); i++) {
			final var column = columnFor(table, columnNames.get(i).toString());
			if (column != null && columnValues.get(i) instanceof Term.Raw term) {
				collectInsertValue(table, term, column, markers);
			}
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

	@SuppressWarnings("unchecked")
	private static void collectUpdate(final SeaStarTable table, final ParsedUpdate raw,
		final NavigableMap<Integer, ColumnDefinition> markers) {
		final List<Pair<Object, Object>> updates = Reflections.getDeclaredField(raw, "updates",
			List.class).orElseThrow();
		for (final var update : updates) {
			final var column = columnFor(table, update.left.toString());
			if (column != null && update.right instanceof Operation.SetValue setValue) {
				Reflections.getDeclaredField(setValue, "value", Term.Raw.class)
					.ifPresent(term -> putIfMarker(term, column, markers));
			}
		}
		collectWhere(table, whereRelations(raw), markers);
	}

	private static void collectSelect(final SeaStarTable table,
		final SelectStatement.RawStatement raw,
		final NavigableMap<Integer, ColumnDefinition> markers) {
		if (raw.whereClause != null) {
			collectWhere(table, raw.whereClause.relations, markers);
		}
		if (raw.limit instanceof AbstractMarker.Raw) {
			putIfMarker(raw.limit, syntheticDefinition(table, "[limit]", DataTypes.INT), markers);
		}
	}

	private static void collectWhere(final SeaStarTable table, final List<Relation> relations,
		final NavigableMap<Integer, ColumnDefinition> markers) {
		for (final var relation : relations) {
			if (!(relation instanceof SingleColumnRelation single)) {
				continue;
			}
			final var column = columnFor(table, single.getEntity().toString());
			if (column == null) {
				continue;
			}
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

	private static ColumnDefinitions resolveSelectResult(final SeaStarTable table,
		final List<RawSelector> selectClause) {
		if (selectClause.isEmpty()) {
			return table.snapshot();
		}

		final List<ColumnDefinition> columns = new ArrayList<>(selectClause.size());
		for (final var selector : selectClause) {
			if (!(selector.selectable instanceof Selectable.RawIdentifier)) {
				return EmptyColumnDefinitions.INSTANCE;
			}
			final var text = Reflections.getDeclaredField(selector.selectable, "text", String.class)
				.orElse(null);
			final var column = text == null ? null : columnFor(table, text);
			if (column == null) {
				return EmptyColumnDefinitions.INSTANCE;
			}
			columns.add(column);
		}

		return DefaultColumnDefinitions.valueOf(columns);
	}

	private static List<Relation> whereRelations(final Object raw) {
		return Reflections.getDeclaredField(raw, "whereClause", WhereClause.class)
			.map(where -> where.relations)
			.orElseGet(List::of);
	}

	private static ColumnDefinition columnFor(final SeaStarTable table, final String name) {
		final var index = table.firstIndexOf(CqlIdentifier.fromInternal(name));

		return index < 0 ? null : table.get(index);
	}

	private static void putIfMarker(final Term.Raw term, final ColumnDefinition column,
		final NavigableMap<Integer, ColumnDefinition> markers) {
		if (term instanceof AbstractMarker.Raw marker) {
			Reflections.getDeclaredField(marker, "bindIndex", Integer.class)
				.ifPresent(bindIndex -> markers.put(bindIndex, column));
		}
	}

	private static ColumnDefinitions toDefinitions(
		final NavigableMap<Integer, ColumnDefinition> markers) {
		if (markers.isEmpty()) {
			return EmptyColumnDefinitions.INSTANCE;
		}
		// Bind indices are assigned sequentially by the parser; a gap means a marker we could not map,
		// so the metadata cannot be reliably indexed by position.
		if (markers.firstKey() != 0 || markers.lastKey() != markers.size() - 1) {
			return EmptyColumnDefinitions.INSTANCE;
		}

		return DefaultColumnDefinitions.valueOf(new ArrayList<>(markers.values()));
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
