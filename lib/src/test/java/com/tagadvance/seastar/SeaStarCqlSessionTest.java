package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlSession;

class SeaStarCqlSessionTest extends AbstractCqlSessionTest {

	@Override
	protected CqlSession createInstance() {
		return SeaStarCqlSession.builder().build();
	}

}
