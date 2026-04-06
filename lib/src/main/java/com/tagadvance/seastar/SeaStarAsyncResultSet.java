package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import org.jspecify.annotations.NonNull;

public class SeaStarAsyncResultSet implements AsyncResultSet {

	private final ColumnDefinitions definitions;
	private final ExecutionInfo executionInfo;
	private final CountingIterator<Row> iterator;
	private final Iterable<Row> currentPage;

	public SeaStarAsyncResultSet(final @NonNull ColumnDefinitions definitions,
		final @NonNull ExecutionInfo executionInfo, final @NonNull Queue<Row> data) {
		this.definitions = requireNonNull(definitions, "definitions must not be null");
		this.executionInfo = requireNonNull(executionInfo, "executionInfo must not be null");
		requireNonNull(data, "data must not be null");

		this.iterator = new CountingIterator<>(data.size()) {
			@Override
			protected Row computeNext() {
				final var rowData = data.poll();
				return rowData == null ? endOfData() : rowData;
			}
		};
		this.currentPage = () -> iterator;
	}

	@NonNull
	@Override
	public ColumnDefinitions getColumnDefinitions() {
		return definitions;
	}

	@NonNull
	@Override
	public ExecutionInfo getExecutionInfo() {
		return executionInfo;
	}

	@NonNull
	@Override
	public Iterable<Row> currentPage() {
		return currentPage;
	}

	@Override
	public int remaining() {
		return iterator.remaining();
	}

	@Override
	public boolean hasMorePages() {
		// keep it simple by returning all data on the first page
		return false;
	}

	@NonNull
	@Override
	public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
		throw new IllegalStateException(
			"No next page. Use #hasMorePages before calling this method to avoid this error.");
	}

	@Override
	public boolean wasApplied() {
		if (iterator.hasNext()) {
			return true;
		} else {
			// preserve functionality from DefaultAsyncResultSet
			throw new IllegalStateException(
				"This method must be called before consuming all the rows");
		}
	}

	public static AsyncResultSet empty(final ExecutionInfo executionInfo) {
		return new AsyncResultSet() {
			@NonNull
			@Override
			public ColumnDefinitions getColumnDefinitions() {
				return EmptyColumnDefinitions.INSTANCE;
			}

			@NonNull
			@Override
			public ExecutionInfo getExecutionInfo() {
				return executionInfo;
			}

			@NonNull
			@Override
			public Iterable<Row> currentPage() {
				return Collections.emptyList();
			}

			@Override
			public int remaining() {
				return 0;
			}

			@Override
			public boolean hasMorePages() {
				return false;
			}

			@NonNull
			@Override
			public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
				throw new IllegalStateException(
					"No next page. Use #hasMorePages before calling this method to avoid this error.");
			}

			@Override
			public boolean wasApplied() {
				return true;
			}
		};
	}

}
