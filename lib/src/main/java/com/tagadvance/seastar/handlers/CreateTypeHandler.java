package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.VolatileUserDefinedType;
import com.tagadvance.seastar.VolatileUserDefinedType.UserDefinedTypeDefinition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.cql3.statements.schema.CreateTypeStatement.Raw;

public class CreateTypeHandler implements CqlHandler<Raw> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public CreateTypeHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof Raw;
	}

	@Override
	@SuppressWarnings("unchecked")
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Raw raw, final Object... bindings) {
		final var name = Reflections.getDeclaredField(raw, "name", UTName.class).orElseThrow();
		final var keyspace = Optional.of(name)
			.map(UTName::getKeyspace)
			.orElseGet(() -> getKeyspace.get().map(CqlIdentifier::asInternal).orElse(null));
		final var udtName = name.getStringTypeName();
		final var ifNotExists = Reflections.getDeclaredField(raw, "ifNotExists", Boolean.class)
			.orElse(false);
		final List<FieldIdentifier> fieldNames = Reflections.getDeclaredField(raw, "fieldNames",
			List.class).orElseGet(Collections::emptyList);
		final List<SeaStarRawType> fieldTypes = Reflections.getDeclaredField(raw, "rawFieldTypes",
				List.class)
			.orElseGet(Collections::emptyList)
			.stream()
			.map(SeaStarRawType::from)
			.toList();

		if (keyspace == null) {
			throw new InvalidQueryException(executionInfo.getCoordinator(),
				"No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
		}

		final var optionalKeyspace = context.getSeaStarKeyspace(keyspace);
		if (optionalKeyspace.isEmpty()) {
			throw new InvalidQueryException(executionInfo.getCoordinator(),
				"Keyspace '%s' does not exist".formatted(keyspace));
		}

		final var ksx = optionalKeyspace.get();
		final var optionalUdt = ksx.getSeaStarUserDefinedType(udtName);
		if (optionalUdt.isPresent()) {
			if (ifNotExists) {
				LOG.debug("User define type {} in keyspace {} already exists, skipping creation",
					udtName, keyspace);
			} else {
				return CompletableFuture.failedStage(
					new InvalidQueryException(executionInfo.getCoordinator(),
						"A user type with name '%s' already exists".formatted(udtName)));
			}
		} else {
			final var definitions = new ArrayList<UserDefinedTypeDefinition>(fieldNames.size());
			for (int i = 0; i < fieldNames.size(); i++) {
				final var fieldName = CqlIdentifier.fromInternal(fieldNames.get(i).toString());
				final var dataType = fieldTypes.get(i)
					.toDataType(ksx, executionInfo.getCoordinator());
				if (dataType.isEmpty()) {
					return CompletableFuture.failedStage(
						new InvalidQueryException(executionInfo.getCoordinator(),
							"Unknown type for field '%s'".formatted(fieldNames.get(i))));
				}
				definitions.add(new UserDefinedTypeDefinition(fieldName, dataType.get()));
			}
			// Frozen is a property of the referencing column, not the stored type.
			final var udt = new VolatileUserDefinedType(context, ksx.name(),
				CqlIdentifier.fromInternal(udtName), false, definitions);
			ksx.putSeaStarUserDefinedType(udtName, udt);
		}

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

}
