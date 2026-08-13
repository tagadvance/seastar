package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import java.net.InetSocketAddress;
import java.time.Duration;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The one Cassandra node every {@code Container*FidelityTest} class runs against. One container
 * rather than one per class, because a node takes tens of seconds to boot and the fidelity groups
 * write to disjoint keyspaces precisely so they can share one - concurrently included. Started on
 * first use and never stopped; Testcontainers' resource reaper removes it when the JVM exits.
 */
final class CassandraContainers {

	private static final int PORT = 9042;

	/**
	 * Pinned to the same version as the {@code cassandra-all} dependency; bump both together.
	 */
	private static final DockerImageName IMAGE = DockerImageName.parse("cassandra:5.0.8");

	private static final CassandraContainer CASSANDRA = new CassandraContainer(IMAGE);

	/**
	 * The driver's default {@code basic.request.timeout} is two seconds, which a containerised node
	 * exceeds on DDL often enough to be a recurring flake: schema agreement across a cold node is
	 * simply slower than that, especially while another suite is competing for the machine. Raising
	 * it costs nothing in fidelity - a timeout is a transport concern, and SeaStar accepts and
	 * ignores timeouts anyway - and it stops a green suite from failing for reasons unrelated to
	 * behavior.
	 */
	private static final DriverConfigLoader CONFIG = DriverConfigLoader.programmaticBuilder()
		.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
		.withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT,
			Duration.ofSeconds(30))
		.build();

	private CassandraContainers() {
		// hidden constructor
	}

	static CqlSession newSession() {
		synchronized (CASSANDRA) {
			if (!CASSANDRA.isRunning()) {
				CASSANDRA.start();
			}
		}

		return CqlSession.builder()
			.addContactPoint(
				new InetSocketAddress(CASSANDRA.getHost(), CASSANDRA.getMappedPort(PORT)))
			.withLocalDatacenter(CASSANDRA.getLocalDatacenter())
			.withConfigLoader(CONFIG)
			.build();
	}

}
