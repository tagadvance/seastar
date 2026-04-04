package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy.ReconnectionSchedule;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.time.Duration;
import org.jspecify.annotations.NonNull;

/**
 * A {@link ReconnectionPolicy} that always returns {@link Duration#ZERO}.
 */
public final class NoDelayReconnectionPolicy implements ReconnectionPolicy, ReconnectionSchedule {

	@Override
	@NonNull
	public ReconnectionSchedule newNodeSchedule(final @NonNull Node node) {
		return this;
	}

	@Override
	@NonNull
	public ReconnectionSchedule newControlConnectionSchedule(final boolean isInitialConnection) {
		return this;
	}

	@Override
	public void close() {
		// do nothing
	}

	@Override
	@NonNull
	public Duration nextDelay() {
		return Duration.ZERO;
	}

}
