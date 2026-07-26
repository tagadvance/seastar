package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Tag;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the shared fidelity suite against a real Cassandra node. Excluded from the default
 * {@code test} task; run it with {@code ./gradlew :lib:containerTest}. Skipped rather than failed
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

	@Override
	protected CqlSession createInstance() {
		return CqlSession.builder()
			.addContactPoint(
				new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(PORT)))
			.withLocalDatacenter(cassandra.getLocalDatacenter())
			.build();
	}

}
