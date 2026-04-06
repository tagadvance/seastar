package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.tagadvance.tools.SeaStarReadWriteLock;

public interface SeaStarUserDefinedType extends SeaStarReadWriteLock, UserDefinedType {

	SeaStarDriverContext context();

	// TODO: mutability

}
