package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlSession;

class SeaStarQueryFidelityTest extends AbstractQueryFidelityTest {

	@Override
	protected CqlSession createInstance() {
		return SeaStarCqlSession.builder().build();
	}

}
