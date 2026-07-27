package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import java.util.Arrays;
import java.util.Objects;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SeaStarRequestProcessorRegistry} is analogous to {@link RequestProcessorRegistry}.
 */
@ThreadSafe
class SeaStarRequestProcessorRegistry {

	private static final Logger LOG = LoggerFactory.getLogger(
		SeaStarRequestProcessorRegistry.class);

	private final String logPrefix;
	private final SeaStarRequestProcessor<?, ?>[] processors;

	public SeaStarRequestProcessorRegistry(final @NonNull String logPrefix,
		final @NonNull SeaStarRequestProcessor<?, ?>... processors) {
		this.logPrefix = requireNonNull(logPrefix, "logPrefix must not be null");
		this.processors = requireNonNull(processors, "processors must not be null");
		Arrays.stream(processors).forEach(Objects::requireNonNull);
	}

	/**
	 * The processor for a request and the result type it was asked for.
	 *
	 * <p>Deliberately {@link IllegalArgumentException} rather than a driver exception, because that is
	 * what {@link RequestProcessorRegistry#processorFor} itself throws: reaching this point means a
	 * caller asked for a result type no processor was registered for, which is a programming error on
	 * the client side rather than a query the server rejected. The message names the request's type
	 * and the result type instead of printing the request, whose {@code toString} is an identity
	 * hash.
	 *
	 * @throws IllegalArgumentException if no processor handles the pair
	 */
	@SuppressWarnings("unchecked")
	public <RequestT extends Request, ResultT> SeaStarRequestProcessor<RequestT, ResultT> processorFor(
		RequestT request, GenericType<ResultT> resultType) {
		for (final var processor : processors) {
			if (processor.canProcess(request, resultType)) {
				LOG.trace("[{}] Using {} to process {}", logPrefix, processor, request);

				return (SeaStarRequestProcessor<RequestT, ResultT>) processor;
			} else {
				LOG.trace("[{}] {} cannot process {}, trying next", logPrefix, processor, request);
			}
		}

		throw new IllegalArgumentException(
			"No request processor found for a %s asked for as %s".formatted(
				request.getClass().getSimpleName(), resultType));
	}

}
