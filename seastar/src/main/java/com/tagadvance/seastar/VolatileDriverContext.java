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
import com.tagadvance.tools.SeaStarReadWriteLock;
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
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;

/**
 * The root of the storage model, and the outermost lock of the hierarchy.
 *
 * <p>Its lock guards the keyspace map alone: creating or dropping a keyspace takes it for writing,
 * and every statement that operates inside a keyspace holds it for reading so that the keyspace
 * cannot vanish underneath it. See the lock hierarchy in {@code AGENTS.md}.
 */
@ThreadSafe
class VolatileDriverContext extends DefaultDriverContext implements SeaStarDriverContext {

	private static final AtomicInteger SESSION_NAME_COUNTER = new AtomicInteger();

	/**
	 * Immutable.
	 */
	private final Node node = new VolatileNode();

	/**
	 * The outermost lock of the hierarchy. Immutable; see {@link SeaStarReadWriteLock#lock()}.
	 */
	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	/**
	 * Immutable.
	 */
	private final String sessionName;
	@GuardedBy("lock")
	private final Map<CqlIdentifier, SeaStarKeyspace> keyspaceById;
	/**
	 * Immutable; {@link Clock} is required to be thread-safe.
	 */
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

	/**
	 * Always empty: with one node reached in process there is nothing to balance, and no policy is
	 * ever consulted. Note the driver's contract promises an entry for the default profile; SeaStar
	 * deliberately breaks that, so {@code getLoadBalancingPolicy(profileName)} answers {@code null}.
	 */
	@Override
	@NonNull
	public Map<String, LoadBalancingPolicy> getLoadBalancingPolicies() {
		return Collections.emptyMap();
	}

	/**
	 * Always empty: a request either succeeds or throws the failure a node would have sent - there
	 * is no transient network error to retry through. As with the load balancing policies, the
	 * default profile entry the contract promises is deliberately absent.
	 */
	@Override
	@NonNull
	public Map<String, RetryPolicy> getRetryPolicies() {
		return Collections.emptyMap();
	}

	/**
	 * Always empty: speculating means racing a second node, and there is no second node. As with the
	 * load balancing policies, the default profile entry the contract promises is deliberately
	 * absent.
	 */
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

	/**
	 * Translation exists for reaching nodes across network topologies, and no address is ever dialed
	 * here. The driver already ships the "leave it alone" answer as
	 * {@link PassThroughAddressTranslator}; any configured translator is ignored.
	 */
	@Override
	@NonNull
	public AddressTranslator getAddressTranslator() {
		return new PassThroughAddressTranslator(this);
	}

	/**
	 * Always empty, even if authentication was configured: there is no connection to authenticate.
	 */
	@Override
	@NonNull
	public Optional<AuthProvider> getAuthProvider() {
		return Optional.empty();
	}

	/**
	 * Always empty, even if SSL was configured: there is no socket to encrypt.
	 */
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

	/**
	 * The version this session's codecs encode and decode values with. It is deliberately
	 * <strong>not</strong> the version of any socket, and must not be made to track one.
	 *
	 * <p>An in-process session is on no protocol at all: nothing here is ever framed. What the value
	 * is used for is choosing a codec's serialization format, and every type SeaStar supports has
	 * encoded identically since v3 - which is what lets {@code seastar-server} re-serve these bytes
	 * unchanged to a client on any version it speaks.
	 *
	 * <p>Nor could it name a wire version even if it wanted to. One session can back a listener
	 * serving a v4 connection and a v5 connection at the same time, so there is no single version
	 * that would be true of it; the version is per connection and lives there. A client reached over
	 * the wire reads its own driver context, never this one.
	 *
	 * <p>Raising it would therefore change no encoding, but it would start claiming protocol
	 * features - per-request keyspaces, {@code now_in_seconds}, modern framing - that the in-process
	 * path does not implement and that driver code gates on the version to decide about.
	 * {@link ProtocolVersion#DEFAULT} is also what the real driver uses for a detached value, so a
	 * detached {@code UdtValue} compares like for like.
	 */
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
		return readLockUnchecked(() -> Optional.ofNullable(keyspaceById.get(id)));
	}

	public void putSeaStarKeyspace(final SeaStarKeyspace keyspace) {
		writeLockUnchecked(() -> keyspaceById.put(keyspace.name(), keyspace));
	}

	public void removeSeaStarKeyspace(final CqlIdentifier id) {
		writeLockUnchecked(() -> keyspaceById.remove(id));
	}

	/**
	 * A snapshot of the keyspaces, taken under the read lock. A live view would let a caller iterating
	 * it meet a concurrent {@code CREATE KEYSPACE} halfway - {@code keyspaceById} is a plain
	 * {@link HashMap}, so that is a {@code ConcurrentModificationException} rather than a stale read.
	 */
	public Map<CqlIdentifier, SeaStarKeyspace> getSeaStarKeyspaces() {
		return readLockUnchecked(() -> Map.copyOf(keyspaceById));
	}

	/**
	 * The single node, keyed by its host id; it never changes for the life of the session.
	 */
	@Override
	@NonNull
	public Map<UUID, Node> getNodes() {
		return Map.of(node.getHostId(), node);
	}

	/**
	 * A snapshot, built on {@link #getSeaStarKeyspaces()}. Unlike a live driver's metadata it is
	 * never disabled or incomplete - the metadata being read is the store itself.
	 */
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
