package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Tag;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("container")
@Testcontainers(disabledWithoutDocker = true)
class ContainerValueFidelityTest extends AbstractValueFidelityTest {

	@Override
	protected CqlSession createInstance() {
		return CassandraContainers.newSession();
	}

}
