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
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * The {@link ExecutionInfo} for a statement answered in memory. Most of what the driver's contract
 * describes - retries, speculation, paging, tracing, a wire - is machinery a single-node fake that
 * answers on the calling thread does not have, so those methods return the value the driver defines
 * for "none"; each says so on the method.
 */
@Immutable
class SeaStarExecutionInfo implements ExecutionInfo {

	private final Node coordinator;
	private final Statement<?> statement;

	public SeaStarExecutionInfo(final Node coordinator, final Statement<?> statement) {
		this.coordinator = requireNonNull(coordinator, "coordinator must not be null");
		this.statement = requireNonNull(statement, "statement must not be null");
	}

	@Override
	public Statement<?> getStatement() {
		return statement;
	}

	/**
	 * The session's single node, always: every statement is answered in process, so there is only
	 * one coordinator it could be.
	 */
	@Override
	public Node getCoordinator() {
		return coordinator;
	}

	/**
	 * SeaStar never speculates - the answer is computed inline on the calling thread, so there is no
	 * slow node to hedge against - which the driver defines as a count of zero.
	 *
	 * @return {@code 0}, always
	 */
	@Override
	public int getSpeculativeExecutionCount() {
		return 0;
	}

	/**
	 * The initial execution always answers; there are no speculative executions to beat it.
	 *
	 * @return {@code 0}, always
	 */
	@Override
	public int getSuccessfulExecutionIndex() {
		return 0;
	}

	/**
	 * With one node and no retries, a statement either succeeds or throws - no errors from previous
	 * coordinators can accumulate.
	 *
	 * @return an empty list, always
	 */
	@Override
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

	/**
	 * SeaStar never issues a server-side warning: anything a cluster would warn about is either
	 * answered cleanly or refused with an exception, in keeping with the fidelity goal of failing
	 * loudly rather than approximating quietly.
	 *
	 * @return an empty list, always
	 */
	@Override
	public List<String> getWarnings() {
		return List.of();
	}

	/**
	 * There is no server to send a custom payload back, so the payload is always empty - the same
	 * answer a live cluster gives when nothing set one.
	 */
	@Override
	public Map<String, ByteBuffer> getIncomingPayload() {
		return Map.of();
	}

	/**
	 * A single in-memory node cannot disagree with itself about the schema.
	 *
	 * @return {@code true}, always
	 */
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
	public CompletionStage<QueryTrace> getQueryTraceAsync() {
		return CompletableFuture.failedFuture(
			new IllegalStateException("Tracing was disabled for this request"));
	}

	/**
	 * There is no protocol frame to measure - the rows never cross a wire - so this is the
	 * {@code -1} the driver defines as "information not available".
	 */
	@Override
	public int getResponseSizeInBytes() {
		return -1;
	}

	/**
	 * {@code -1}, always, for the same reason as {@link #getResponseSizeInBytes()}.
	 */
	@Override
	public int getCompressedResponseSizeInBytes() {
		return -1;
	}

}
