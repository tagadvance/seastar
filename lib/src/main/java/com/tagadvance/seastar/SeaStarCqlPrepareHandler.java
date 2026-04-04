package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareHandler;
import java.util.concurrent.CompletableFuture;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SeaStarCqlPrepareHandler} is analogous to {@link CqlPrepareHandler}.
 */
@ThreadSafe
public class SeaStarCqlPrepareHandler {

	private static final Logger LOG = LoggerFactory.getLogger(SeaStarCqlPrepareHandler.class);

	private final String logPrefix;
	private final PrepareRequest initialRequest;
	private final SeaStarDriverContext context;

	protected SeaStarCqlPrepareHandler(final PrepareRequest request,
		final SeaStarDriverContext context, final String sessionLogPrefix) {
		this.initialRequest = request;
		this.context = context;
		this.logPrefix = "%s|%d".formatted(sessionLogPrefix, hashCode());
		LOG.trace("[{}] Creating new handler for prepare request {}", logPrefix, request);
	}

	public CompletableFuture<SeaStarPreparedStatement> handle() {
		final var statement = new SeaStarPreparedStatement(context, initialRequest);
		LOG.debug("[{}] Successfully prepared statement: {}", logPrefix, statement);

		return CompletableFuture.completedFuture(statement);
	}

}
