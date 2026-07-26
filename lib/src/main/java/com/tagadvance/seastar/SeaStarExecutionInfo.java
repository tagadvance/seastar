package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public class SeaStarExecutionInfo implements ExecutionInfo {

	private final Node coordinator;
	private final Statement<?> statement;

	public SeaStarExecutionInfo(final Node coordinator, final Statement<?> statement) {
		this.coordinator = requireNonNull(coordinator, "coordinator must not be null");
		this.statement = requireNonNull(statement, "statement must not be null");
	}

	@Override
	@NonNull
	public Statement<?> getStatement() {
		return statement;
	}

	@Override
	public Node getCoordinator() {
		return coordinator;
	}

	@Override
	public int getSpeculativeExecutionCount() {
		return 0;
	}

	@Override
	public int getSuccessfulExecutionIndex() {
		return 0;
	}

	@Override
	@NonNull
	public List<Entry<Node, Throwable>> getErrors() {
		return Collections.emptyList();
	}

	/**
	 * SeaStar answers every query from memory in a single page, so there is never a next page to
	 * fetch.
	 *
	 * @return {@code null}, always, which the driver defines as "there is no next page"
	 */
	@Override
	@Nullable
	public ByteBuffer getPagingState() {
		return null;
	}

	@Override
	@NonNull
	public List<String> getWarnings() {
		return List.of();
	}

	/**
	 * There is no server to send a custom payload back, so the payload is always empty - the same
	 * answer a live cluster gives when nothing set one.
	 */
	@Override
	@NonNull
	public Map<String, ByteBuffer> getIncomingPayload() {
		return Map.of();
	}

	@Override
	public boolean isSchemaInAgreement() {
		return true;
	}

	/**
	 * SeaStar never traces: there is no coordinator to record a trace and no {@code system_traces}
	 * keyspace to read it back from. Tracing is therefore always disabled, which the driver reports
	 * as a {@code null} tracing id.
	 *
	 * @return {@code null}, always
	 */
	@Override
	@Nullable
	public UUID getTracingId() {
		return null;
	}

	/**
	 * Fails the same way the real driver does when {@link #getTracingId()} is {@code null}, so client
	 * code that fetches a trace sees the familiar failure instead of an
	 * {@link UnsupportedOperationException}.
	 *
	 * @return a stage failed with {@link IllegalStateException}, always
	 */
	@Override
	@NonNull
	public CompletionStage<QueryTrace> getQueryTraceAsync() {
		return CompletableFuture.failedFuture(
			new IllegalStateException("Tracing was disabled for this request"));
	}

	@Override
	public int getResponseSizeInBytes() {
		return -1;
	}

	@Override
	public int getCompressedResponseSizeInBytes() {
		return -1;
	}

}
