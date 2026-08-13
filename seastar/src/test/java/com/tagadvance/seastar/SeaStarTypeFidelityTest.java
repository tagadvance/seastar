package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlSession;

class SeaStarTypeFidelityTest extends AbstractTypeFidelityTest {

	@Override
	protected CqlSession createInstance() {
		return SeaStarCqlSession.builder().build();
	}

}
