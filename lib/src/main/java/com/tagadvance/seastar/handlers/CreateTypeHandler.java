package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarKeyspace;
import com.tagadvance.seastar.VolatileUserDefinedType;
import com.tagadvance.seastar.VolatileUserDefinedType.UserDefinedTypeDefinition;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTypeStatement.Raw;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTypeHandler implements CqlHandler<Raw> {

	private static final Logger LOG = LoggerFactory.getLogger(CreateTypeHandler.class);

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public CreateTypeHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof Raw;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Raw raw, final Object... bindings) {
		final var name = FieldBindings.CREATE_TYPE_NAME.require(raw);
		final var udtName = name.getStringTypeName();
		final var ifNotExists = FieldBindings.CREATE_TYPE_IF_NOT_EXISTS.require(raw);
		final var fieldNames = FieldBindings.CREATE_TYPE_FIELD_NAMES.require(raw);
		final var fieldTypes = FieldBindings.CREATE_TYPE_RAW_FIELD_TYPES.require(raw)
			.stream()
			.map(SeaStarRawType::new)
			.toList();

		final SeaStarKeyspace ksx;
		try {
			ksx = Targets.requireKeyspace(context, getKeyspace, name.getKeyspace(),
				executionInfo.getCoordinator());
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}
		final var keyspace = ksx.name().asInternal();
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
			final var udt = new VolatileUserDefinedType(context, ksx,
				CqlIdentifier.fromInternal(udtName), false, definitions);
			ksx.putSeaStarUserDefinedType(udt);
		}

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

}
