package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy.ReconnectionSchedule;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.specex.NoSpeculativeExecutionPolicy;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;

public final class SeaStarDriverContext extends DefaultDriverContext {

	private static final AtomicInteger SESSION_NAME_COUNTER = new AtomicInteger();

	private final String sessionName;

	public SeaStarDriverContext(final DriverConfigLoader configLoader,
		final ProgrammaticArguments programmaticArguments) {
		super(configLoader, programmaticArguments);

		DriverExecutionProfile defaultProfile = configLoader.getInitialConfig().getDefaultProfile();
		if (defaultProfile.isDefined(DefaultDriverOption.SESSION_NAME)) {
			this.sessionName = defaultProfile.getString(DefaultDriverOption.SESSION_NAME);
		} else {
			this.sessionName = "seastar%d".formatted(SESSION_NAME_COUNTER.getAndIncrement());
		}
	}

	@Override
	@NonNull
	public String getSessionName() {
		return sessionName;
	}

	@Override
	@NonNull
	public Map<String, LoadBalancingPolicy> getLoadBalancingPolicies() {
		return Maps.alwaysReturn(new NullLoadBalancingPolicy());
	}

	@Override
	@NonNull
	public Map<String, RetryPolicy> getRetryPolicies() {
		return Maps.alwaysReturn(new NoRetryPolicy());
	}

	@Override
	@NonNull
	public Map<String, SpeculativeExecutionPolicy> getSpeculativeExecutionPolicies() {
		return Maps.alwaysReturn(new NoSpeculativeExecutionPolicy(null, null));
	}

	@Override
	@NonNull
	public ReconnectionPolicy getReconnectionPolicy() {
		return new NullReconnectionPolicy();
	}

	@Override
	@NonNull
	public AddressTranslator getAddressTranslator() {
		return new NullAddressTranslator();
	}

	@Override
	@NonNull
	public Optional<AuthProvider> getAuthProvider() {
		return Optional.empty();
	}

	@Override
	@NonNull
	public Optional<SslEngineFactory> getSslEngineFactory() {
		return Optional.empty();
	}

	@Override
	@NonNull
	public RequestThrottler getRequestThrottler() {
		return new NullRequestThrottler();
	}

	@Override
	@NonNull
	public NodeStateListener getNodeStateListener() {
		return new NullNodeStateListener();
	}

	private static final class NullLoadBalancingPolicy implements LoadBalancingPolicy {

		@Override
		public void init(final @NonNull Map<UUID, Node> nodes,
			final @NonNull DistanceReporter distanceReporter) {
			// dp nothing
		}

		@Override
		@NonNull
		public Queue<Node> newQueryPlan(final Request request, final Session session) {
			return new LinkedList<>();
		}

		@Override
		public void onAdd(final @NonNull Node node) {
			// do nothing
		}

		@Override
		public void onUp(final @NonNull Node node) {
			// do nothing
		}

		@Override
		public void onDown(final @NonNull Node node) {
			// do nothing
		}

		@Override
		public void onRemove(final @NonNull Node node) {
			// do nothing
		}

		@Override
		public void close() {
			// do nothing
		}

	}

	private static final class NoRetryPolicy implements RetryPolicy {

		@Override
		public RetryDecision onReadTimeout(final @NonNull Request request,
			final @NonNull ConsistencyLevel cl, final int blockFor, final int received,
			final boolean dataPresent, final int retryCount) {
			return RetryDecision.IGNORE;
		}

		@Override
		public RetryDecision onWriteTimeout(final @NonNull Request request,
			final @NonNull ConsistencyLevel cl, final @NonNull WriteType writeType,
			final int blockFor, final int received, final int retryCount) {
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

	private static final class NullReconnectionPolicy implements ReconnectionPolicy,
		ReconnectionSchedule {

		@Override
		@NonNull
		public ReconnectionSchedule newNodeSchedule(final @NonNull Node node) {
			return this;
		}

		@Override
		@NonNull
		public ReconnectionSchedule newControlConnectionSchedule(
			final boolean isInitialConnection) {
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

	private static final class NullAddressTranslator implements AddressTranslator {

		@Override
		@NonNull
		public InetSocketAddress translate(final @NonNull InetSocketAddress address) {
			return address;
		}

		@Override
		public void close() {
			// do nothing
		}

	}

	private static final class NullRequestThrottler implements RequestThrottler {

		@Override
		public void register(final @NonNull Throttled request) {
			// do nothing
		}

		@Override
		public void signalSuccess(final @NonNull Throttled request) {
			// do nothing
		}

		@Override
		public void signalError(final @NonNull Throttled request, final @NonNull Throwable error) {
			// do nothing
		}

		@Override
		public void signalTimeout(final @NonNull Throttled request) {
			// do nothing
		}

		@Override
		public void close() {
			// do nothing
		}

	}

	private static final class NullNodeStateListener implements NodeStateListener {

		@Override
		public void onAdd(final @NonNull Node node) {
			// do nothing
		}

		@Override
		public void onUp(final @NonNull Node node) {
			// do nothing
		}

		@Override
		public void onDown(final @NonNull Node node) {
			// do nothing
		}

		@Override
		public void onRemove(final @NonNull Node node) {
			// do nothing
		}

		@Override
		public void close() {
			// do nothing
		}

	}

}
