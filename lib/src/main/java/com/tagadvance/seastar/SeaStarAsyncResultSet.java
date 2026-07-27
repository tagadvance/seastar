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
import net.jcip.annotations.NotThreadSafe;
import org.jspecify.annotations.NonNull;

/**
 * A result set that is always a single page.
 *
 * <p><strong>SeaStar does not page, deliberately.</strong> A cluster pages because a result set may
 * not fit in memory and because the rows are on another machine; SeaStar's rows are already in this
 * process, on this thread, so a page boundary would be an invention with nothing behind it.
 * {@link #hasMorePages()} is therefore always {@code false}, {@link #fetchNextPage()} always throws
 * {@link IllegalStateException} - which is what the driver's own contract says an implementation
 * does when there is no next page - and {@code setPageSize} on a statement is accepted and has no
 * effect.
 *
 * <p>The idioms client code is written in keep working, because every one of them terminates on the
 * first page: {@code while (rs.hasMorePages())} runs zero times, {@code rs.all()},
 * {@code rs.iterator()} and {@code rs.currentPage()} return every row, and
 * {@code ResultSet#getAvailableWithoutFetching()} equals the total. What is <em>not</em> reproduced
 * is code that asserts on the page boundary itself - the number of pages, a page size being
 * respected, or {@code fetchNextPage()} returning something. {@code AbstractCqlSessionTest} pins the
 * idioms on both backends.
 *
 * <p>Not safe for concurrent use: {@link #currentPage()} and {@code rs.iterator()} share one
 * underlying {@link CountingIterator}, the same as any standard {@link java.util.Iterator} would.
 */
@NotThreadSafe
public class SeaStarAsyncResultSet implements AsyncResultSet {

	private final ColumnDefinitions definitions;
	private final ExecutionInfo executionInfo;
	private final CountingIterator<Row> iterator;
	private final Iterable<Row> currentPage;
	private final Row firstRow;

	public SeaStarAsyncResultSet(final @NonNull ColumnDefinitions definitions,
		final @NonNull ExecutionInfo executionInfo, final @NonNull Queue<Row> data) {
		this.definitions = requireNonNull(definitions, "definitions must not be null");
		this.executionInfo = requireNonNull(executionInfo, "executionInfo must not be null");
		requireNonNull(data, "data must not be null");
		this.firstRow = data.peek();

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

	/**
	 * @return {@code false}, always; every row is on the first page
	 */
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
		// Mirror DefaultAsyncResultSet: only a lightweight transaction's [applied] column can
		// report false; every other result set is considered applied.
		if (firstRow == null || !definitions.contains("[applied]")) {
			return true;
		}

		return firstRow.getBoolean("[applied]");
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
