package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import java.net.InetSocketAddress;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the shared fidelity suite against a real Cassandra node. Excluded from the default
 * {@code test} task; run it with {@code ./gradlew :seastar:containerTest}. Skipped rather than failed
 * when no Docker daemon is reachable.
 */
@Tag("container")
@Testcontainers(disabledWithoutDocker = true)
class ContainerCqlSessionTest extends AbstractCqlSessionTest {

	private static final int PORT = 9042;

	/**
	 * Pinned to the same version as the {@code cassandra-all} dependency; bump both together.
	 */
	private static final DockerImageName IMAGE = DockerImageName.parse("cassandra:5.0.8");

	@Container
	private static final CassandraContainer cassandra = new CassandraContainer(IMAGE);

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

	@Override
	protected CqlSession createInstance() {
		return CqlSession.builder()
			.addContactPoint(
				new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(PORT)))
			.withLocalDatacenter(cassandra.getLocalDatacenter())
			.withConfigLoader(CONFIG)
			.build();
	}

}
