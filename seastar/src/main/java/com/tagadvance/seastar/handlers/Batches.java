package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * The apply phase both batch paths share - {@link BatchHandler} for a batch parsed from CQL, the
 * request handler for a driver-built {@code BatchStatement}. See {@link BatchHandler} for the
 * semantics.
 */
public final class Batches {

	private Batches() {

	}

	/**
	 * Runs each translated child in order while holding the write lock of every keyspace the batch
	 * names, taken in name order so two batches cannot deadlock and released in reverse. The
	 * suppliers complete synchronously (see {@link CqlHandler.Translated}), so every child has
	 * applied before the locks are released and a concurrent reader sees the batch entirely or not
	 * at all.
	 *
	 * @param translated the children, already validated by translation
	 * @param result     the batch's own result, built once every child has applied
	 * @return the result, or the first child's failure
	 */
	public static CompletionStage<AsyncResultSet> apply(
		final List<CqlHandler.Translated> translated, final Supplier<AsyncResultSet> result) {
		final var keyspaces = translated.stream()
			.map(CqlHandler.Translated::keyspaces)
			.flatMap(Set::stream)
			.distinct()
			.sorted(Comparator.comparing(keyspace -> keyspace.name().asInternal()))
			.toList();

		keyspaces.forEach(keyspace -> keyspace.lock().writeLock().lock());
		try {
			for (final var child : translated) {
				final var stage = child.apply().get().toCompletableFuture();
				if (stage.isCompletedExceptionally()) {
					// Only reachable for a failure translation could not catch - nothing rolls
					// back, but nothing that plain validation rejects gets this far either.
					return stage;
				}
			}
		} finally {
			for (int i = keyspaces.size() - 1; i >= 0; i--) {
				keyspaces.get(i).lock().writeLock().unlock();
			}
		}

		return CompletableFuture.completedStage(result.get());
	}

}
