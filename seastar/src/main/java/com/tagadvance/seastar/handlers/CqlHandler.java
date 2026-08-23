package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import com.tagadvance.seastar.SeaStarAsyncResultSet;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarKeyspace;
import com.tagadvance.seastar.SeaStarRow;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.cql3.CQLStatement;

public interface CqlHandler<T extends CQLStatement.Raw> {

	boolean canProcess(final CQLStatement.Raw raw);

	CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, T raw, final Object... bindings);

	/**
	 * A statement translated but not yet applied: the keyspaces its application will write, and the
	 * deferred application itself. Validation happens during translation, so a {@code Translated}
	 * that was handed out does not fail for a reason validation could have caught - which is what
	 * lets a batch validate every child before applying any. The supplier completes synchronously;
	 * a batch relies on that to release its locks only after every child has applied.
	 */
	record Translated(Set<SeaStarKeyspace> keyspaces,
					  Supplier<CompletionStage<AsyncResultSet>> apply) {

	}

	/**
	 * Splits {@link #processCql} into its two halves - validate now, apply later - for the caller
	 * that must know the first cannot fail before running the second: a batch. Throws eagerly on a
	 * validation failure rather than returning a failed stage, so that caller can tell "invalid"
	 * apart from "applied".
	 *
	 * <p>The default does not split: it defers the whole of {@link #processCql} and names no
	 * keyspace, which is only correct for a statement that never reaches a batch. The parser limits
	 * batch children to INSERT, UPDATE and DELETE, and those three handlers override this with a
	 * real split.
	 */
	default Translated translateCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final T raw, final Object... bindings) {
		return new Translated(Set.of(), () -> processCql(context, executionInfo, raw, bindings));
	}

	default AsyncResultSet newAsyncResultSet(final ExecutionInfo executionInfo) {
		return newAsyncResultSet(EmptyColumnDefinitions.INSTANCE, executionInfo, Stream.empty());
	}

	default AsyncResultSet newAsyncResultSet(final ColumnDefinitions columnDefinitions,
		final ExecutionInfo executionInfo, final Stream<SeaStarRow> rows) {
		final var data = rows.map(SeaStarRow::snapshot)
			.collect(Collectors.toCollection(LinkedList::new));

		return new SeaStarAsyncResultSet(columnDefinitions, executionInfo, data);
	}

}
