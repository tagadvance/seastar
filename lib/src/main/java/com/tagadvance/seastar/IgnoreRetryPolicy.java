package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import org.jspecify.annotations.NonNull;

/**
 * A {@link RetryPolicy} that always returns {@link RetryDecision#IGNORE}.
 */
public final class IgnoreRetryPolicy implements RetryPolicy {

	@Override
	public RetryDecision onReadTimeout(final @NonNull Request request,
		final @NonNull ConsistencyLevel cl, final int blockFor, final int received,
		final boolean dataPresent, final int retryCount) {
		return RetryDecision.IGNORE;
	}

	@Override
	public RetryDecision onWriteTimeout(final @NonNull Request request,
		final @NonNull ConsistencyLevel cl, final @NonNull WriteType writeType, final int blockFor,
		final int received, final int retryCount) {
		return RetryDecision.IGNORE;
	}

	@Override
	public RetryDecision onUnavailable(final @NonNull Request request,
		final @NonNull ConsistencyLevel cl, final int required, final int alive,
		final int retryCount) {
		return RetryDecision.IGNORE;
	}

	@Override
	public RetryDecision onRequestAborted(final @NonNull Request request,
		final @NonNull Throwable error, final int retryCount) {
		return RetryDecision.IGNORE;
	}

	@Override
	public RetryDecision onErrorResponse(final @NonNull Request request,
		final @NonNull CoordinatorException error, final int retryCount) {
		return RetryDecision.IGNORE;
	}

	@Override
	public void close() {
		// do nothing
	}

}
