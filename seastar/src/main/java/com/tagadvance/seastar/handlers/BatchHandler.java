package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.BatchStatement.Parsed;

/**
 * Handles a {@code BEGIN BATCH ... APPLY BATCH} parsed from a CQL string. Each child statement is
 * a {@code ModificationStatement.Parsed} (INSERT/UPDATE/DELETE); the parser rejects anything else,
 * so no SELECT can reach here. Children are dispatched to their own handlers in order, mirroring a
 * reasonable first cut of batch semantics for an in-memory fake: apply each child in sequence.
 *
 * <p>Known limitation: this is not atomic. Real Cassandra validates every child up front and
 * rejects the whole batch before applying anything, so an invalid statement leaves the store
 * untouched. Here each child is validated and applied as it is dispatched, so a child that fails
 * partway through (for example an undefined column) leaves the earlier children already applied
 * rather than rolling the batch back. Batches are also not isolated - see
 * {@code docs/support-matrix.md}. Each child takes and releases its own table lock rather than
 * holding all of them for the batch, which is also why a two-table batch cannot deadlock; making
 * a batch atomic would mean locking every child's table up front, sorted by a stable key.
 */
@ThreadSafe
public class BatchHandler implements CqlHandler<Parsed> {

	private final Supplier<CqlHandlerRegistry> registry;

	public BatchHandler(final Supplier<CqlHandlerRegistry> registry) {
		this.registry = requireNonNull(registry, "registry must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof Parsed;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Parsed raw, final Object... bindings) {
		// A batch-level USING applies its timestamp or TTL to every child that does not carry one of
		// its own. Nothing here reads it, so it is refused rather than dropped: a child written with
		// no USING would otherwise be stamped with the clock and read back with a writetime the batch
		// said it should not have.
		final var attributes = FieldBindings.BATCH_ATTRIBUTES.require(raw);
		if (attributes.timestamp != null || attributes.timeToLive != null) {
			return CompletableFuture.failedStage(
				new InvalidQueryException(executionInfo.getCoordinator(),
					"SeaStar does not support USING on a BATCH; write it on each statement instead"));
		}

		final var children = FieldBindings.BATCH_STATEMENTS.require(raw);

		final var registry = this.registry.get();
		CompletionStage<AsyncResultSet> chain = CompletableFuture.completedStage(null);
		for (final var child : children) {
			chain = chain.thenCompose(
				ignored -> registry.processorFor(child, executionInfo)
					.processCql(context, executionInfo, child, bindings));
		}

		return chain.thenApply(ignored -> newAsyncResultSet(executionInfo));
	}

}
