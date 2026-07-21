package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class ContainerCqlSessionTest extends AbstractCqlSessionTest {

	private static final int PORT = 9042;

	@Container
	private static final CassandraContainer cassandra = new CassandraContainer(
		DockerImageName.parse("cassandra"));

	@Override
	protected CqlSession createInstance() {
		return CqlSession.builder()
			.addContactPoint(
				new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(PORT)))
			.withLocalDatacenter(cassandra.getLocalDatacenter())
			.build();
	}

}
