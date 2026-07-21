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
import org.jspecify.annotations.NonNull;

public class VolatileNode implements Node {

	private static final String DATACENTER = "datacenter1";
	private static final String RACK = "rack1";

	private final UUID HOST_ID = UUID.randomUUID();
	private final UUID SCHEMA_VERSION = UUID.randomUUID();

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

	@Override
	@NonNull
	public Optional<InetSocketAddress> getBroadcastRpcAddress() {
		return Optional.empty();
	}

	@Override
	@NonNull
	public Optional<InetSocketAddress> getBroadcastAddress() {
		return Optional.empty();
	}

	@Override
	@NonNull
	public Optional<InetSocketAddress> getListenAddress() {
		return Optional.empty();
	}

	@Override
	public String getDatacenter() {
		return DATACENTER;
	}

	@Override
	public String getRack() {
		return RACK;
	}

	@Override
	public Version getCassandraVersion() {
		return Version.V6_9_0;
	}

	@Override
	@NonNull
	public Map<String, Object> getExtras() {
		return Collections.emptyMap();
	}

	@Override
	@NonNull
	public NodeState getState() {
		return NodeState.UP;
	}

	@Override
	public long getUpSinceMillis() {
		return 0;
	}

	@Override
	public int getOpenConnections() {
		return 1;
	}

	@Override
	public boolean isReconnecting() {
		return false;
	}

	@Override
	@NonNull
	public NodeDistance getDistance() {
		return NodeDistance.LOCAL;
	}

	@Override
	public UUID getHostId() {
		return HOST_ID;
	}

	@Override
	public UUID getSchemaVersion() {
		return SCHEMA_VERSION;
	}

}
