package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.Collections;
import java.util.List;
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
	@SuppressWarnings("unchecked")
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Parsed raw, final Object... bindings) {
		final List<CQLStatement.Raw> children = Reflections.getDeclaredField(raw, "parsedStatements",
			List.class).orElseGet(Collections::emptyList);

		final var registry = this.registry.get();
		CompletionStage<AsyncResultSet> chain = CompletableFuture.completedStage(null);
		for (final var child : children) {
			chain = chain.thenCompose(
				ignored -> registry.processorFor(child).processCql(context, executionInfo, child,
					bindings));
		}

		return chain.thenApply(ignored -> newAsyncResultSet(executionInfo));
	}

}
