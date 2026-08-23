package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.BatchStatement.Parsed;

/**
 * Handles a {@code BEGIN BATCH ... APPLY BATCH} parsed from a CQL string. Each child statement is
 * a {@code ModificationStatement.Parsed} (INSERT/UPDATE/DELETE); the parser rejects anything else,
 * so no SELECT can reach here.
 *
 * <p>Atomic and isolated, in two phases. Every child is translated - which is where validation
 * lives - before any is applied, so an invalid child fails the batch with the store untouched,
 * as a node's up-front validation does. The applications then run in order while this holds the
 * write lock of every keyspace the batch touches, taken in name order so that a two-keyspace
 * batch cannot deadlock; a concurrent reader sees the batch entirely or not at all. The one
 * carve-out, documented in {@code docs/support-matrix.md}: a conditional child's {@code IF} is
 * evaluated as its turn comes, against the state its predecessors left, where a node evaluates
 * every condition against the pre-batch state.
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
		final var translated = new ArrayList<Translated>(children.size());
		try {
			for (final var child : children) {
				translated.add(registry.processorFor(child, executionInfo)
					.translateCql(context, executionInfo, child, bindings));
			}
		} catch (final RuntimeException e) {
			return CompletableFuture.failedStage(e);
		}

		return Batches.apply(translated, () -> newAsyncResultSet(executionInfo));
	}

}
