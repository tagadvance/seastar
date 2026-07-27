package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

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
import com.datastax.oss.driver.internal.core.metadata.NoopNodeStateListener;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import com.datastax.oss.driver.internal.core.session.throttling.PassThroughRequestThrottler;
import com.google.errorprone.annotations.ThreadSafe;
import java.time.Clock;
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

	private final Node node = new VolatileNode();

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private final String sessionName;
	private final Map<CqlIdentifier, SeaStarKeyspace> keyspaceById;
	private final Clock clock;

	public VolatileDriverContext(final DriverConfigLoader configLoader,
		final ProgrammaticArguments programmaticArguments) {
		this(configLoader, programmaticArguments, Clock.systemUTC());
	}

	/**
	 * @param clock the clock write times are stamped with and TTLs expire against; pass a
	 *              {@link SeaStarClock} to move it by hand
	 */
	public VolatileDriverContext(final DriverConfigLoader configLoader,
		final ProgrammaticArguments programmaticArguments, final Clock clock) {
		super(configLoader, programmaticArguments);
		this.clock = requireNonNull(clock, "clock must not be null");

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

	/**
	 * SeaStar dispatches through {@link SeaStarRequestProcessorRegistry}, whose processors take a
	 * {@link SeaStarCqlSession} rather than the driver's {@code DefaultSession}. The driver's own
	 * registry cannot be built here and would never be consulted, so there is no empty answer to
	 * give.
	 *
	 * @throws UnsupportedOperationException always
	 */
	@Override
	protected RequestProcessorRegistry buildRequestProcessorRegistry() {
		throw new UnsupportedOperationException(
			"SeaStar does not support the driver's RequestProcessorRegistry; it dispatches through SeaStarRequestProcessorRegistry");
	}

	/**
	 * @throws UnsupportedOperationException always
	 * @see #buildRequestProcessorRegistry()
	 */
	@Override
	@NonNull
	public RequestProcessorRegistry getRequestProcessorRegistry() {
		throw new UnsupportedOperationException(
			"SeaStar does not support the driver's RequestProcessorRegistry; it dispatches through SeaStarRequestProcessorRegistry");
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

	/**
	 * SeaStar holds no connection, so nothing can ever drop and there is no reconnection to schedule.
	 * The driver's {@link ReconnectionPolicy} contract hands out retry delays, and there is no
	 * meaningful delay to invent for a session that never reconnects.
	 *
	 * @throws UnsupportedOperationException always
	 */
	@Override
	@NonNull
	public ReconnectionPolicy getReconnectionPolicy() {
		throw new UnsupportedOperationException(
			"SeaStar does not support reconnection policies; it holds no connection to reconnect");
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

	/**
	 * Requests are answered from memory on the calling thread, so there is nothing to throttle. The
	 * driver already ships the "no limit" answer as {@link PassThroughRequestThrottler}.
	 */
	@Override
	@NonNull
	public RequestThrottler getRequestThrottler() {
		return new PassThroughRequestThrottler(this);
	}

	/**
	 * SeaStar's single node never changes state, so a listener would never be notified. The driver
	 * already ships the "nobody is listening" answer as {@link NoopNodeStateListener}.
	 */
	@Override
	@NonNull
	public NodeStateListener getNodeStateListener() {
		return new NoopNodeStateListener(this);
	}

	@Override
	@NonNull
	public ProtocolVersion getProtocolVersion() {
		return ProtocolVersion.DEFAULT;
	}

	@Override
	public Node getNode() {
		return node;
	}

	@Override
	public Clock getClock() {
		return clock;
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
	@SuppressWarnings("all")
	public Map<UUID, Node> getNodes() {
		return Map.of(node.getHostId(), node);
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, KeyspaceMetadata> getKeyspaces() {
		return getSeaStarKeyspaces().entrySet()
			.stream()
			.collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	/**
	 * SeaStar models a single node with no token ring: there is nothing to partition data across, so
	 * there are no token ranges and no replicas to compute. The driver's contract already allows for
	 * an absent token map (it is also empty on a real cluster while schema metadata is disabled), so
	 * an empty {@link Optional} is the honest answer.
	 *
	 * @return {@link Optional#empty()}, always
	 */
	@Override
	@NonNull
	public Optional<TokenMap> getTokenMap() {
		return Optional.empty();
	}

}
