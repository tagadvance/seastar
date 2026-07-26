package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.tagadvance.tools.SeaStarReadWriteLock;
import java.util.Map;

public interface SeaStarUserDefinedType extends SeaStarReadWriteLock, UserDefinedType {

	SeaStarDriverContext context();

	/**
	 * Appends a field. {@code ALTER TYPE ... ADD} only ever appends, so values written before the
	 * alter keep their positions and read back null for the new field.
	 */
	void addField(CqlIdentifier name, DataType dataType);

	/**
	 * Renames fields, keyed by their current name. Applied as one unit so that names swapping between
	 * two existing fields resolve against the original names rather than each other.
	 */
	void renameFields(Map<CqlIdentifier, CqlIdentifier> renames);

}
