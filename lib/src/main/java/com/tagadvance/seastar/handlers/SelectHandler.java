package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.tagadvance.seastar.SeaStarAsyncResultSet;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarTable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.statements.SelectStatement.RawStatement;
import org.jspecify.annotations.NonNull;

public class SelectHandler implements CqlHandler<RawStatement> {

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof RawStatement;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final RawStatement raw, final Object... bindings) {
		final var coordinator = executionInfo.getCoordinator();

		if (raw.parameters.isDistinct) {
			// TODO: add support for distinct
			LOG.warn("DISTINCT is not supported, ignoring");
		}

		final var optionalKeyspace = context.getSeaStarKeyspace(
			CqlIdentifier.fromInternal(raw.keyspace()));
		if (optionalKeyspace.isEmpty()) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"Keyspace '%s' does not exist".formatted(raw.keyspace())));
		}

		final var optionalTable = optionalKeyspace.get()
			.getSeaStarTable(CqlIdentifier.fromInternal(raw.name()));
		if (optionalTable.isEmpty()) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"table %s does not exist".formatted(raw.name())));
		}
		final var table = optionalTable.get();
		final var codecRegistry = context.getCodecRegistry();

		final Projection projection;
		final Predicate<SeaStarRow> predicate;
		final Integer limit;
		try {
			projection = resolveProjection(table, raw.selectClause, coordinator);
			final var relations = raw.whereClause == null ? List.<Relation>of()
				: raw.whereClause.relations;
			predicate = resolveWhere(table, raw.parameters.allowFiltering, relations, codecRegistry,
				coordinator, bindings);
			limit = resolveLimit(raw.limit, codecRegistry, coordinator, bindings);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		return table.readLockUnchecked(() -> {
			var rows = table.rows();
			if (predicate != null) {
				rows = rows.filter(predicate);
			}
			if (limit != null) {
				rows = rows.limit(limit);
			}
			final Stream<Row> snapshots = rows.map(SeaStarRow::snapshot);
			final Stream<Row> projected = projection == null ? snapshots
				: snapshots.map(row -> project(row, projection));
			final var data = projected.collect(Collectors.toCollection(LinkedList::new));
			final var definitions = projection == null ? table.snapshot() : projection.definitions();

			return CompletableFuture.<AsyncResultSet>completedStage(
				new SeaStarAsyncResultSet(definitions, executionInfo, data));
		});
	}

	private record Projection(ColumnDefinitions definitions, int[] indices) {

	}

	private static Projection resolveProjection(final SeaStarTable table,
		final List<RawSelector> selectClause, final Node coordinator) {
		if (selectClause.isEmpty()) {
			return null;
		}

		final List<ColumnDefinition> columns = new ArrayList<>();
		final var indices = new int[selectClause.size()];
		for (int i = 0; i < selectClause.size(); i++) {
			final var selectable = selectClause.get(i).selectable;
			if (!(selectable instanceof Selectable.RawIdentifier)) {
				throw new UnsupportedOperationException(
					"Unsupported select item %s".formatted(selectable));
			}
			final var text = Reflections.getDeclaredField(selectable, "text", String.class)
				.orElseThrow();
			final var index = table.firstIndexOf(CqlIdentifier.fromInternal(text));
			if (index < 0) {
				throw new InvalidQueryException(coordinator,
					"Undefined column name %s".formatted(text));
			}
			indices[i] = index;
			columns.add(table.get(index));
		}

		return new Projection(DefaultColumnDefinitions.valueOf(columns), indices);
	}

	private static Predicate<SeaStarRow> resolveWhere(final SeaStarTable table,
		final boolean allowFiltering, final List<Relation> relations,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		if (relations.isEmpty()) {
			return null;
		}

		final var primaryKey = primaryKeyNames(table);
		final List<Predicate<SeaStarRow>> predicates = new ArrayList<>();
		for (final var relation : relations) {
			if (!(relation instanceof SingleColumnRelation single)) {
				throw new UnsupportedOperationException(
					"Unsupported relation %s".formatted(relation));
			}
			final var text = Reflections.getDeclaredField(single.getEntity(), "text", String.class)
				.orElseThrow();
			final var name = CqlIdentifier.fromInternal(text);
			final var index = table.firstIndexOf(name);
			if (index < 0) {
				throw new InvalidQueryException(coordinator,
					"Undefined column name %s".formatted(text));
			}
			if (!primaryKey.contains(name) && !allowFiltering) {
				throw new InvalidQueryException(coordinator,
					"Cannot execute this query as it might involve data filtering and thus may have "
						+ "unpredictable performance. If you want to execute this query despite the "
						+ "performance unpredictability, use ALLOW FILTERING");
			}

			final var dataType = table.get(index).getType();
			if (relation.isEQ()) {
				final var target = resolveTerm(single.getValue(), dataType, codecRegistry, bindings);
				predicates.add(row -> Objects.equals(row.getObject(index), target));
			} else if (relation.isIN()) {
				final Set<Object> targets = new HashSet<>();
				for (final var term : single.getInValues()) {
					targets.add(resolveTerm(term, dataType, codecRegistry, bindings));
				}
				predicates.add(row -> targets.contains(row.getObject(index)));
			} else {
				throw new UnsupportedOperationException(
					"Unsupported operator %s in WHERE".formatted(relation.operator()));
			}
		}

		return predicates.stream().reduce(Predicate::and).orElseThrow();
	}

	private static Integer resolveLimit(final Term.Raw limit, final CodecRegistry codecRegistry,
		final Node coordinator, final Object... bindings) {
		if (limit == null) {
			return null;
		}

		final var value = resolveTerm(limit, DataTypes.INT, codecRegistry, bindings);
		if (!(value instanceof Number number)) {
			throw new InvalidQueryException(coordinator, "Invalid limit value %s".formatted(value));
		}
		final var n = number.intValue();
		if (n <= 0) {
			throw new InvalidQueryException(coordinator, "LIMIT must be strictly positive");
		}

		return n;
	}

	private static Object resolveTerm(final Term.Raw term, final DataType dataType,
		final CodecRegistry codecRegistry, final Object... bindings) {
		if (term instanceof AbstractMarker.Raw marker) {
			final var bindIndex = Reflections.getDeclaredField(marker, "bindIndex", Integer.class)
				.orElseThrow();

			return bindIndex < bindings.length ? bindings[bindIndex] : null;
		} else if (term instanceof Constants.Literal literal) {
			return codecRegistry.codecFor(dataType).parse(literal.getText());
		}

		throw new UnsupportedOperationException("Unsupported term %s".formatted(term));
	}

	private static Set<CqlIdentifier> primaryKeyNames(final SeaStarTable table) {
		final Set<CqlIdentifier> names = new HashSet<>();
		table.getPartitionKey().stream().map(ColumnMetadata::getName).forEach(names::add);
		table.getClusteringColumns().keySet().stream().map(ColumnMetadata::getName)
			.forEach(names::add);

		return names;
	}

	private static Row project(final Row source, final Projection projection) {
		final var definitions = projection.definitions();
		final var indices = projection.indices();

		return new Row() {

			@Override
			public boolean isDetached() {
				return source.isDetached();
			}

			@Override
			public void attach(final @NonNull AttachmentPoint attachmentPoint) {
				throw new UnsupportedOperationException();
			}

			@Override
			@NonNull
			public CodecRegistry codecRegistry() {
				return source.codecRegistry();
			}

			@Override
			@NonNull
			public ProtocolVersion protocolVersion() {
				return source.protocolVersion();
			}

			@Override
			public int size() {
				return definitions.size();
			}

			@Override
			@NonNull
			public DataType getType(final int i) {
				return definitions.get(i).getType();
			}

			@Override
			public ByteBuffer getBytesUnsafe(final int i) {
				return source.getBytesUnsafe(indices[i]);
			}

			@Override
			public int firstIndexOf(final @NonNull String name) {
				return definitions.firstIndexOf(name);
			}

			@Override
			@NonNull
			public DataType getType(final @NonNull String name) {
				return definitions.get(name).getType();
			}

			@Override
			public int firstIndexOf(final @NonNull CqlIdentifier id) {
				return definitions.firstIndexOf(id);
			}

			@Override
			@NonNull
			public DataType getType(final @NonNull CqlIdentifier id) {
				return definitions.get(id).getType();
			}

			@Override
			@NonNull
			public ColumnDefinitions getColumnDefinitions() {
				return definitions;
			}

		};
	}

}
