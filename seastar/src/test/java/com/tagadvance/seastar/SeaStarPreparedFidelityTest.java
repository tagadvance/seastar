package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlSession;

class SeaStarPreparedFidelityTest extends AbstractPreparedFidelityTest {

	@Override
	protected CqlSession createInstance() {
		return SeaStarCqlSession.builder().build();
	}

}
