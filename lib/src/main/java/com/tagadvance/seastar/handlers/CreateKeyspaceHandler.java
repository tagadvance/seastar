package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement.Raw;
import org.apache.cassandra.schema.KeyspaceParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CreateKeyspaceHandler implements CqlHandler<CreateKeyspaceStatement.Raw> {

	private static final Logger LOG = LoggerFactory.getLogger(CreateKeyspaceHandler.class);

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof CreateKeyspaceStatement.Raw;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Raw raw, final Object... bindings) {
		final var name = raw.keyspaceName;
		final var ifNotExists = FieldBindings.CREATE_KEYSPACE_IF_NOT_EXISTS.require(raw);
		final var optionalKeyspace = context.getSeaStarKeyspace(name);
		if (optionalKeyspace.isPresent()) {
			if (ifNotExists) {
				LOG.debug("Keyspace {} already exists, skipping creation", name);
			} else {
				return CompletableFuture.failedStage(
					new AlreadyExistsException(executionInfo.getCoordinator(), name, null));
			}
		} else {
			final var attrs = FieldBindings.CREATE_KEYSPACE_ATTRIBUTES.require(raw);
			final var durableWrites = attrs.getBoolean(
				KeyspaceParams.Option.DURABLE_WRITES.toString(),
				KeyspaceParams.DEFAULT_DURABLE_WRITES);

			context.newSeaStarKeyspace(CqlIdentifier.fromInternal(name),
				Replication.of(attrs).orElse(SeaStarDriverContext.DEFAULT_REPLICATION),
				durableWrites);
		}

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

}
