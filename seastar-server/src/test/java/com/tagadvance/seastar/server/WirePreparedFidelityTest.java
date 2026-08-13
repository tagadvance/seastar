package com.tagadvance.seastar.server;

import com.datastax.oss.driver.api.core.CqlSession;
import com.tagadvance.seastar.AbstractPreparedFidelityTest;
import org.junit.jupiter.api.extension.RegisterExtension;

class WirePreparedFidelityTest extends AbstractPreparedFidelityTest {

	@RegisterExtension
	final WireHarness harness = new WireHarness();

	@Override
	protected CqlSession createInstance() {
		return harness.createInstance();
	}

}
