package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.NonNull;

/**
 * The single node SeaStar reports. Every field is immutable and nothing about a node ever changes,
 * so it takes part in no lock: it is outside the hierarchy in {@code AGENTS.md} rather than at the
 * bottom of it.
 */
@Immutable
class VolatileNode implements Node {

	private static final String DATACENTER = "datacenter1";
	private static final String RACK = "rack1";

	private final UUID HOST_ID = UUID.randomUUID();
	private final UUID SCHEMA_VERSION = UUID.randomUUID();

	/**
	 * A nominal loopback endpoint - nothing listens on it and nothing ever connects to it. The
	 * contract requires an endpoint and a metric prefix, so the node names the address a
	 * single-node cluster would have had.
	 */
	@Override
	@NonNull
	public EndPoint getEndPoint() {
		return new EndPoint() {
			@Override
			@NonNull
			public SocketAddress resolve() {
				return InetSocketAddress.createUnresolved("127.0.0.1", 9042);
			}

			@Override
			@NonNull
			public String asMetricPrefix() {
				return "127_0_0_1_9042";
			}
		};
	}

	/**
	 * Always empty - SeaStar binds no address, and the contract already allows this to be unknown.
	 */
	@Override
	@NonNull
	public Optional<InetSocketAddress> getBroadcastRpcAddress() {
		return Optional.empty();
	}

	/**
	 * Always empty, for the same reason as {@link #getBroadcastRpcAddress()}.
	 */
	@Override
	@NonNull
	public Optional<InetSocketAddress> getBroadcastAddress() {
		return Optional.empty();
	}

	/**
	 * Always empty, for the same reason as {@link #getBroadcastRpcAddress()}.
	 */
	@Override
	@NonNull
	public Optional<InetSocketAddress> getListenAddress() {
		return Optional.empty();
	}

	/**
	 * Always {@code datacenter1}, the stock SimpleSnitch answer - the same name a fresh container
	 * reports, so driver configuration written against one fits the other.
	 */
	@Override
	public String getDatacenter() {
		return DATACENTER;
	}

	/**
	 * Always {@code rack1}, the stock SimpleSnitch answer; see {@link #getDatacenter()}.
	 */
	@Override
	public String getRack() {
		return RACK;
	}

	/**
	 * A constant. SeaStar is not any particular Cassandra release, and nothing in the in-process
	 * path consults the version.
	 */
	@Override
	public Version getCassandraVersion() {
		return Version.V6_9_0;
	}

	@Override
	@NonNull
	public Map<String, Object> getExtras() {
		return Collections.emptyMap();
	}

	/**
	 * Always {@link NodeState#UP}: the node is the process itself, so it is up for exactly as long
	 * as anything exists to ask.
	 */
	@Override
	@NonNull
	public NodeState getState() {
		return NodeState.UP;
	}

	/**
	 * The epoch, as a constant: the node never transitions state, so there is no real timestamp to
	 * report.
	 */
	@Override
	public long getUpSinceMillis() {
		return 0;
	}

	/**
	 * Always 1. There are no connections in process, but zero is what a driver shows for a node it
	 * has lost, and this node is never lost.
	 */
	@Override
	public int getOpenConnections() {
		return 1;
	}

	/**
	 * Always false: with no connections there is nothing to lose, so there is never anything to
	 * reconnect.
	 */
	@Override
	public boolean isReconnecting() {
		return false;
	}

	/**
	 * Always {@link NodeDistance#LOCAL}: distance is a load-balancing concept, and with a single
	 * node no other answer would let requests through.
	 */
	@Override
	@NonNull
	public NodeDistance getDistance() {
		return NodeDistance.LOCAL;
	}

	@Override
	public UUID getHostId() {
		return HOST_ID;
	}

	/**
	 * Fixed at construction and never moved by DDL. Nothing in process checks schema agreement;
	 * {@code seastar-server} keeps its own moving {@code schema_version} in {@code system.local} for
	 * connected drivers that do.
	 */
	@Override
	public UUID getSchemaVersion() {
		return SCHEMA_VERSION;
	}

}
