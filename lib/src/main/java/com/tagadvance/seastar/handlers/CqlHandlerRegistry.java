package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Objects;
import org.apache.cassandra.cql3.CQLStatement;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlHandlerRegistry {

	private static final Logger LOG = LoggerFactory.getLogger(CqlHandlerRegistry.class);

	private final String logPrefix;
	private final CqlHandler<?>[] processors;

	public CqlHandlerRegistry(final @NonNull String logPrefix,
		final @NonNull CqlHandler<?>... processors) {
		this.logPrefix = requireNonNull(logPrefix, "logPrefix must not be null");
		this.processors = requireNonNull(processors, "processors must not be null");
		Arrays.stream(processors).forEach(Objects::requireNonNull);
	}

	@SuppressWarnings("unchecked")
	public <S extends CQLStatement.Raw> CqlHandler<S> processorFor(final S statement) {
		for (final var processor : processors) {
			if (processor.canProcess(statement)) {
				LOG.trace("[{}] Using {} to process {}", logPrefix, processor, statement);

				return (CqlHandler<S>) processor;
			} else {
				LOG.trace("[{}] {} cannot process {}, trying next", logPrefix, processor,
					statement);
			}
		}

		throw new IllegalArgumentException("No request processor found for " + statement);
	}

}
