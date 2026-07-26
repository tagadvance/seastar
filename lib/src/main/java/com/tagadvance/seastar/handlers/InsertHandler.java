package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarRow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedInsert;

public class InsertHandler implements CqlHandler<ParsedInsert> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public InsertHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof ParsedInsert;
	}

	@Override
	@SuppressWarnings("unchecked")
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final ParsedInsert raw, final Object... bindings) {
		final var coordinator = executionInfo.getCoordinator();

		final var keyspace = Optional.ofNullable(raw.keyspace())
			.or(() -> getKeyspace.get().map(CqlIdentifier::asInternal))
			.orElse(null);
		if (keyspace == null) {
			throw new InvalidQueryException(coordinator,
				"No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
		}

		final var optionalKeyspace = context.getSeaStarKeyspace(
			CqlIdentifier.fromInternal(keyspace));
		if (optionalKeyspace.isEmpty()) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"Keyspace '%s' does not exist".formatted(keyspace)));
		}

		final var optionalTable = optionalKeyspace.get()
			.getSeaStarTable(CqlIdentifier.fromInternal(raw.name()));
		if (optionalTable.isEmpty()) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"table %s does not exist".formatted(raw.name())));
		}
		final var table = optionalTable.get();

		final List<Object> columnNames = Reflections.getDeclaredField(raw, "columnNames", List.class)
			.orElseGet(Collections::emptyList);
		final List<Object> columnValues = Reflections.getDeclaredField(raw, "columnValues",
			List.class).orElseGet(Collections::emptyList);
		final var ifNotExists = Reflections.getDeclaredField(raw, "ifNotExists", Boolean.class)
			.orElse(false);

		final var codecRegistry = context.getCodecRegistry();
		final var values = new ArrayList<Object>(Collections.nCopies(table.size(), null));
		final var named = new HashSet<CqlIdentifier>();
		final var namedIndices = new ArrayList<Integer>(columnNames.size());
		for (int i = 0; i < columnNames.size(); i++) {
			final var name = CqlIdentifier.fromInternal(columnNames.get(i).toString());
			final var index = table.firstIndexOf(name);
			if (index < 0) {
				return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
					"Undefined column name %s".formatted(name.asInternal())));
			}
			named.add(name);
			namedIndices.add(index);

			final var dataType = table.get(index).getType();
			final var term = columnValues.get(i);
			values.set(index, toValue(coordinator, codecRegistry, dataType, term, bindings));
		}

		final List<CqlIdentifier> primaryKey = new ArrayList<>();
		table.getPartitionKey().stream().map(ColumnMetadata::getName).forEach(primaryKey::add);
		table.getClusteringColumns().keySet().stream().map(ColumnMetadata::getName)
			.forEach(primaryKey::add);
		for (final var pk : primaryKey) {
			if (!named.contains(pk)) {
				return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
					"Missing mandatory PRIMARY KEY part %s".formatted(pk.asInternal())));
			}
		}

		final var pkIndices = primaryKey.stream().mapToInt(table::firstIndexOf).toArray();
		final Predicate<SeaStarRow> samePrimaryKey = existing -> {
			for (final var index : pkIndices) {
				if (!Objects.equals(existing.getObject(index), values.get(index))) {
					return false;
				}
			}
			return true;
		};

		final AsyncResultSet result = table.writeLockUnchecked(() -> {
			if (ifNotExists) {
				final var existing = table.rows().filter(samePrimaryKey).findFirst().orElse(null);
				if (existing == null) {
					table.addRow(values);
					return AppliedResultSets.of(context, table, executionInfo, true);
				}
				return AppliedResultSets.ofExisting(context, table, executionInfo, existing.snapshot());
			}
			// INSERT is an upsert; write only the named columns, preserving any columns this
			// statement did not specify on a row that already shares this primary key. An
			// explicitly-inserted NULL still clears its column; an unnamed column is left as-is.
			final var existing = table.rows().filter(samePrimaryKey).findFirst().orElse(null);
			if (existing == null) {
				table.addRow(values);
			} else {
				for (final var index : namedIndices) {
					existing.set(index, values.get(index));
				}
			}
			return newAsyncResultSet(executionInfo);
		});

		return CompletableFuture.completedStage(result);
	}

	// A UDT literal can nest further literals and bind markers, so terms resolve recursively against
	// the type of the field they are being assigned to rather than only at the top level.
	private static Object toValue(final Node coordinator, final CodecRegistry codecRegistry,
		final DataType dataType, final Object term, final Object... bindings) {
		if (term instanceof AbstractMarker.Raw marker) {
			final var bindIndex = Reflections.getDeclaredField(marker, "bindIndex", Integer.class)
				.orElseThrow();

			return bindIndex < bindings.length ? bindings[bindIndex] : null;
		} else if (term instanceof UserTypes.Literal literal) {
			return toUdtValue(coordinator, codecRegistry, dataType, literal, bindings);
		} else if (term instanceof Constants.Literal literal) {
			return codecRegistry.codecFor(dataType).parse(literal.getText());
		}

		throw new UnsupportedOperationException("Unsupported INSERT value %s".formatted(term));
	}

	private static UdtValue toUdtValue(final Node coordinator, final CodecRegistry codecRegistry,
		final DataType dataType, final UserTypes.Literal literal, final Object... bindings) {
		if (!(dataType instanceof UserDefinedType udt)) {
			throw new InvalidQueryException(coordinator,
				"Invalid user type literal for a column of type %s".formatted(dataType.asCql(true, true)));
		}

		// Fields absent from the literal keep the null they were initialised with, matching
		// UserTypes.Literal.prepare, which substitutes NULL_LITERAL for anything not named.
		final var value = udt.newValue();
		literal.entries.forEach((field, term) -> {
			final var name = CqlIdentifier.fromInternal(field.toString());
			final var index = udt.firstIndexOf(name);
			if (index < 0) {
				throw new InvalidQueryException(coordinator,
					"Unknown field '%s' in value of user defined type %s".formatted(name.asInternal(),
						udt.getName().asInternal()));
			}
			final var fieldType = udt.getFieldTypes().get(index);
			final TypeCodec<Object> codec = codecRegistry.codecFor(fieldType);
			value.set(index, toValue(coordinator, codecRegistry, fieldType, term, bindings), codec);
		});

		return value;
	}

}
