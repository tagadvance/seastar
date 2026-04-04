package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import org.jspecify.annotations.NonNull;

public class SeaStarExecutionInfo implements ExecutionInfo {

	private final Statement<?> statement;
	private final List<Throwable> errors;
	private final SeaStarCqlSession session;
	private final SeaStarDriverContext context;

	public SeaStarExecutionInfo(final Statement<?> statement, final List<Throwable> errors,
		final SeaStarCqlSession session, final SeaStarDriverContext context) {
		this.statement = requireNonNull(statement, "statement must not be null");
		this.errors = requireNonNull(errors, "errors must not be null");
		this.session = requireNonNull(session, "session must not be null");
		this.context = requireNonNull(context, "context must not be null");
	}

	@Override
	@NonNull
	public Statement<?> getStatement() {
		return statement;
	}

	@Override
	public Node getCoordinator() {
		return null;
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
		return List.of();
	}

	@Override
	public ByteBuffer getPagingState() {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public List<String> getWarnings() {
		return List.of();
	}

	@Override
	@NonNull
	public Map<String, ByteBuffer> getIncomingPayload() {
		// TODO
		return Map.of();
	}

	@Override
	public boolean isSchemaInAgreement() {
		return true;
	}

	@Override
	public UUID getTracingId() {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CompletionStage<QueryTrace> getQueryTraceAsync() {
		throw new UnsupportedOperationException();
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
