package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.schema.AlterKeyspaceStatement.Raw;
import org.apache.cassandra.schema.KeyspaceParams;

/**
 * Handles {@code ALTER KEYSPACE}. Only the two options SeaStar models - the replication map and
 * {@code durable_writes} - can change, and an option the statement leaves out keeps the value it
 * had, which is what a live node does.
 *
 * <p>Neither option changes an answer: SeaStar stores one copy of every row in one process, so
 * replication is metadata a test may read back and nothing more. The statement is still applied
 * rather than ignored, because reading it back is the whole reason a test runs it.
 */
@ThreadSafe
public class AlterKeyspaceHandler implements CqlHandler<Raw> {

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof Raw;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Raw raw, final Object... bindings) {
		final var name = FieldBindings.ALTER_KEYSPACE_NAME.require(raw);
		final var ifExists = FieldBindings.ALTER_KEYSPACE_IF_EXISTS.require(raw);

		final var keyspace = context.getSeaStarKeyspace(CqlIdentifier.fromInternal(name))
			.orElse(null);
		if (keyspace == null) {
			if (ifExists) {
				return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
			}

			return CompletableFuture.failedStage(new InvalidQueryException(
				executionInfo.getCoordinator(), "Keyspace '%s' doesn't exist".formatted(name)));
		}

		final var attrs = FieldBindings.ALTER_KEYSPACE_ATTRIBUTES.require(raw);
		keyspace.alter(Replication.of(attrs).orElseGet(keyspace::getReplication),
			attrs.getBoolean(KeyspaceParams.Option.DURABLE_WRITES.toString(),
				keyspace.isDurableWrites()));

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

}
