package com.tagadvance.seastar.server;

import com.datastax.oss.driver.api.core.CqlSession;
import com.tagadvance.seastar.AbstractSchemaFidelityTest;
import org.junit.jupiter.api.extension.RegisterExtension;

class WireSchemaFidelityTest extends AbstractSchemaFidelityTest {

	@RegisterExtension
	final WireHarness harness = new WireHarness();

	@Override
	protected CqlSession createInstance() {
		return harness.createInstance();
	}

}
