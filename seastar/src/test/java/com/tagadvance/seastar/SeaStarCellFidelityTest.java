package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlSession;

class SeaStarCellFidelityTest extends AbstractCellFidelityTest {

	@Override
	protected CqlSession createInstance() {
		return SeaStarCqlSession.builder().build();
	}

}
