package com.tagadvance.seastar.server;

import com.datastax.oss.driver.api.core.CqlSession;
import com.tagadvance.seastar.AbstractCellFidelityTest;
import org.junit.jupiter.api.extension.RegisterExtension;

class WireCellFidelityTest extends AbstractCellFidelityTest {

	@RegisterExtension
	final WireHarness harness = new WireHarness();

	@Override
	protected CqlSession createInstance() {
		return harness.createInstance();
	}

}
