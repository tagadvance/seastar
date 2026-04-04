package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.cql.CqlRequestAsyncProcessor;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;

/**
 * {@link SeaStarCqlRequestAsyncProcessor} is analogous to {@link CqlRequestAsyncProcessor}.
 */
@ThreadSafe
public class SeaStarCqlRequestAsyncProcessor implements
	SeaStarRequestProcessor<Statement<?>, CompletionStage<AsyncResultSet>> {

	@Override
	public boolean canProcess(final Request request, final GenericType<?> resultType) {
		return request instanceof Statement && resultType.equals(Statement.ASYNC);
	}

	@Override
	public CompletionStage<AsyncResultSet> process(final Statement<?> request,
		final SeaStarCqlSession session, final SeaStarDriverContext context,
		final String sessionLogPrefix) {
		return new SeaStarCqlRequestHandler(request, session, context, sessionLogPrefix).handle();
	}

	@Override
	public CompletionStage<AsyncResultSet> newFailure(RuntimeException error) {
		return CompletableFutures.failedFuture(error);
	}

}
