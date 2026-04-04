package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.addresstranslation.PassThroughAddressTranslator;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.metadata.NoopNodeStateListener;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import com.datastax.oss.driver.internal.core.session.throttling.PassThroughRequestThrottler;
import com.datastax.oss.driver.internal.core.specex.NoSpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.util.concurrent.LazyReference;
import com.google.errorprone.annotations.ThreadSafe;
import com.tagadvance.tools.Maps;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;

@ThreadSafe
public class SeaStarDriverContext extends DefaultDriverContext {

	private static final AtomicInteger SESSION_NAME_COUNTER = new AtomicInteger();

	protected final SeaStarNode node = new VolatileNode(this);

	private final String sessionName;

	private final LazyReference<SeaStarRequestProcessorRegistry> requestProcessorRegistryRef = new LazyReference<>(
		"seaStarRequestProcessorRegistry", this::buildSeaStarRequestProcessorRegistry,
		cycleDetector);

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

	/**
	 * {@link SeaStarDriverContext} does not support custom request processor registries. This
	 * method will always throw an {@link UnsupportedOperationException}.
	 *
	 * @deprecated please use {@link #buildSeaStarRequestProcessorRegistry()} instead
	 */
	@Override
	protected RequestProcessorRegistry buildRequestProcessorRegistry() {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@link SeaStarDriverContext} does not support custom request processor registries. This
	 * method will always throw an {@link UnsupportedOperationException}.
	 *
	 * @deprecated please use {@link #getSeaStarRequestProcessorRegistry()} instead
	 */
	@Override
	@NonNull
	public RequestProcessorRegistry getRequestProcessorRegistry() {
		throw new UnsupportedOperationException();
	}

	@NonNull SeaStarRequestProcessorRegistry getSeaStarRequestProcessorRegistry() {
		return requestProcessorRegistryRef.get();
	}

	private SeaStarRequestProcessorRegistry buildSeaStarRequestProcessorRegistry() {
		final var processors = SeaStarBuiltInRequestProcessors.createDefaultProcessors(this)
			.toArray(SeaStarRequestProcessor[]::new);

		return new SeaStarRequestProcessorRegistry(getSessionName(), processors);
	}

	@Override
	@NonNull
	public Map<String, LoadBalancingPolicy> getLoadBalancingPolicies() {
		return Maps.alwaysReturn(new NullLoadBalancingPolicy());
	}

	@Override
	@NonNull
	public Map<String, RetryPolicy> getRetryPolicies() {
		return Maps.alwaysReturn(new IgnoreRetryPolicy());
	}

	@Override
	@NonNull
	public Map<String, SpeculativeExecutionPolicy> getSpeculativeExecutionPolicies() {
		return Maps.alwaysReturn(new NoSpeculativeExecutionPolicy(this, null));
	}

	@Override
	@NonNull
	public ReconnectionPolicy getReconnectionPolicy() {
		return new NoDelayReconnectionPolicy();
	}

	@Override
	@NonNull
	public AddressTranslator getAddressTranslator() {
		return new PassThroughAddressTranslator(this);
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
		return new PassThroughRequestThrottler(this);
	}

	@Override
	@NonNull
	public NodeStateListener getNodeStateListener() {
		return new NoopNodeStateListener(this);
	}

}
