package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;

/**
 * {@link SeaStarRequestProcessor} is analogous to {@link RequestProcessor}, but for
 * {@link SeaStarCqlSession} instead of {@link DefaultSession}.
 *
 * @param <RequestT> the type of request accepted
 * @param <ResultT>  the type of result when a request is processed
 * @see RequestProcessor
 */
public interface SeaStarRequestProcessor<RequestT extends Request, ResultT> {

	/**
	 * Whether the processor can produce the given result from the given request.
	 * <p>Processors will be tried in the order they were registered. The first processor for which
	 * this method returns true will be used.
	 */
	boolean canProcess(Request request, GenericType<?> resultType);

	/**
	 * Processes the given request, producing a result.
	 */
	ResultT process(RequestT request, SeaStarCqlSession session, SeaStarDriverContext context,
		String sessionLogPrefix);

	/**
	 * Builds a failed result to directly report the given error.
	 */
	ResultT newFailure(RuntimeException error);

}
