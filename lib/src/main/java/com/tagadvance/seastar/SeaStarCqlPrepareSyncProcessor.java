package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareSyncProcessor;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import java.util.concurrent.CompletableFuture;
import net.jcip.annotations.ThreadSafe;

/**
 * {@link SeaStarCqlPrepareSyncProcessor} is analogous to {@link CqlPrepareSyncProcessor}.
 */
@ThreadSafe
public class SeaStarCqlPrepareSyncProcessor implements
	SeaStarRequestProcessor<PrepareRequest, SeaStarPreparedStatement> {

	private final SeaStarCqlPrepareAsyncProcessor asyncProcessor;

	/**
	 * Note: if you also register a {@link SeaStarCqlPrepareAsyncProcessor} with your session, make
	 * sure that you pass that same instance to this constructor. This is necessary for proper
	 * behavior of the prepared statement cache.
	 */
	public SeaStarCqlPrepareSyncProcessor(final SeaStarCqlPrepareAsyncProcessor asyncProcessor) {
		this.asyncProcessor = requireNonNull(asyncProcessor, "asyncProcessor must not be null");
	}

	@Override
	public boolean canProcess(final Request request, final GenericType<?> resultType) {
		return request instanceof PrepareRequest && resultType.equals(PrepareRequest.SYNC);
	}

	@Override
	public SeaStarPreparedStatement process(final PrepareRequest request,
		final SeaStarCqlSession session, final SeaStarDriverContext context,
		final String sessionLogPrefix) {

		return CompletableFutures.getUninterruptibly(
			asyncProcessor.process(request, session, context, sessionLogPrefix));
	}

	public Cache<PrepareRequest, CompletableFuture<SeaStarPreparedStatement>> getCache() {
		return asyncProcessor.getCache();
	}

	@Override
	public SeaStarPreparedStatement newFailure(final RuntimeException error) {
		throw error;
	}

}
