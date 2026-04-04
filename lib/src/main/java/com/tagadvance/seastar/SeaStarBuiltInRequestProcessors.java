package com.tagadvance.seastar;

import com.datastax.oss.driver.internal.core.session.BuiltInRequestProcessors;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * {@link SeaStarBuiltInRequestProcessors} is analogous to {@link BuiltInRequestProcessors}.
 */
public class SeaStarBuiltInRequestProcessors {

	private SeaStarBuiltInRequestProcessors() {
		// prevent instantiation
	}

	public static List<SeaStarRequestProcessor<?, ?>> createDefaultProcessors(
		final SeaStarDriverContext context) {
		final var processors = new ArrayList<SeaStarRequestProcessor<?, ?>>();
		addBasicProcessors(processors, context);

		return processors;
	}

	public static void addBasicProcessors(final List<SeaStarRequestProcessor<?, ?>> processors,
		final SeaStarDriverContext context) {
		// regular requests (sync and async)
		final var cqlRequestAsyncProcessor = new SeaStarCqlRequestAsyncProcessor();
		processors.add(cqlRequestAsyncProcessor);

		final var cqlRequestSyncProcessor = new SeaStarCqlRequestSyncProcessor(
			cqlRequestAsyncProcessor);
		processors.add(cqlRequestSyncProcessor);

		// prepare requests (sync and async)
		final var cqlPrepareAsyncProcessor = new SeaStarCqlPrepareAsyncProcessor(
			Optional.of(context));
		processors.add(cqlPrepareAsyncProcessor);

		final var cqlPrepareSyncProcessor = new SeaStarCqlPrepareSyncProcessor(
			cqlPrepareAsyncProcessor);
		processors.add(cqlPrepareSyncProcessor);
	}

}
