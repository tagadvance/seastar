package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import java.util.Arrays;
import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SeaStarRequestProcessorRegistry} is analogous to {@link RequestProcessorRegistry}.
 */
@ThreadSafe
public class SeaStarRequestProcessorRegistry {

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

		throw new IllegalArgumentException("No request processor found for " + request);
	}

	/**
	 * This creates a defensive copy on every call, do not overuse.
	 */
	public Iterable<SeaStarRequestProcessor<?, ?>> getProcessors() {
		return ImmutableList.copyOf(processors);
	}

}
