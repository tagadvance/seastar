package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.addresstranslation.PassThroughAddressTranslator;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import com.google.errorprone.annotations.ThreadSafe;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.jspecify.annotations.NonNull;

@ThreadSafe
public class VolatileDriverContext extends DefaultDriverContext implements SeaStarDriverContext {

	private static final AtomicInteger SESSION_NAME_COUNTER = new AtomicInteger();

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private final String sessionName;
	private final Map<CqlIdentifier, SeaStarKeyspace> keyspaceById;

	public VolatileDriverContext(final DriverConfigLoader configLoader,
		final ProgrammaticArguments programmaticArguments) {
		super(configLoader, programmaticArguments);

		final var defaultProfile = configLoader.getInitialConfig().getDefaultProfile();
		if (defaultProfile.isDefined(DefaultDriverOption.SESSION_NAME)) {
			this.sessionName = defaultProfile.getString(DefaultDriverOption.SESSION_NAME);
		} else {
			this.sessionName = "seastar%d".formatted(SESSION_NAME_COUNTER.getAndIncrement());
		}

		this.keyspaceById = new HashMap<>();
	}

	@Override
	public ReadWriteLock lock() {
		return lock;
	}

	@Override
	@NonNull
	public String getSessionName() {
		return sessionName;
	}

	@Override
	protected RequestProcessorRegistry buildRequestProcessorRegistry() {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public RequestProcessorRegistry getRequestProcessorRegistry() {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public Map<String, LoadBalancingPolicy> getLoadBalancingPolicies() {
		return Collections.emptyMap();
	}

	@Override
	@NonNull
	public Map<String, RetryPolicy> getRetryPolicies() {
		return Collections.emptyMap();
	}

	@Override
	@NonNull
	public Map<String, SpeculativeExecutionPolicy> getSpeculativeExecutionPolicies() {
		return Collections.emptyMap();
	}

	@Override
	@NonNull
	public ReconnectionPolicy getReconnectionPolicy() {
		throw new UnsupportedOperationException();
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
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public NodeStateListener getNodeStateListener() {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public ProtocolVersion getProtocolVersion() {
		return ProtocolVersion.DEFAULT;
	}

	@Override
	public Optional<SeaStarKeyspace> getSeaStarKeyspace(final CqlIdentifier id) {
		return readLockUnchecked(() -> Optional.of(id).map(keyspaceById::get));
	}

	public void putSeaStarKeyspace(final CqlIdentifier id, final SeaStarKeyspace keyspace) {
		writeLockUnchecked(() -> keyspaceById.put(id, keyspace));
	}

	public void removeSeaStarKeyspace(final CqlIdentifier id) {
		writeLockUnchecked(() -> keyspaceById.remove(id));
	}

	public Map<CqlIdentifier, SeaStarKeyspace> getSeaStarKeyspaces() {
		return Collections.unmodifiableMap(keyspaceById);
	}

	@Override
	@NonNull
	public Map<UUID, Node> getNodes() {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, KeyspaceMetadata> getKeyspaces() {
		return getSeaStarKeyspaces().entrySet()
			.stream()
			.collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	@Override
	@NonNull
	public Optional<TokenMap> getTokenMap() {
		throw new UnsupportedOperationException();
	}

}
