package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.cql.CqlRequestSyncProcessor;
import com.datastax.oss.driver.internal.core.cql.ResultSets;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import net.jcip.annotations.ThreadSafe;

/**
 * {@link SeaStarRequestProcessor} is analogous to {@link CqlRequestSyncProcessor}.
 */
@ThreadSafe
public class SeaStarCqlRequestSyncProcessor implements
	SeaStarRequestProcessor<Statement<?>, ResultSet> {

	private final SeaStarCqlRequestAsyncProcessor asyncProcessor;

	public SeaStarCqlRequestSyncProcessor(final SeaStarCqlRequestAsyncProcessor asyncProcessor) {
		this.asyncProcessor = requireNonNull(asyncProcessor, "asyncProcessor must not be null");
	}

	@Override
	public boolean canProcess(final Request request, final GenericType<?> resultType) {
		return request instanceof Statement && resultType.equals(Statement.SYNC);
	}

	@Override
	public ResultSet process(final Statement<?> request, final SeaStarCqlSession session,
		final SeaStarDriverContext context, final String sessionLogPrefix) {
		final var firstPage = CompletableFutures.getUninterruptibly(
			asyncProcessor.process(request, session, context, sessionLogPrefix));

		return ResultSets.newInstance(firstPage);
	}

	@Override
	public ResultSet newFailure(final RuntimeException error) {
		throw error;
	}

}
