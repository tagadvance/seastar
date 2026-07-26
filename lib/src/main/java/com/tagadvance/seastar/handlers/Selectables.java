package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import org.apache.cassandra.cql3.selection.Selectable;

/**
 * Converts the identifiers in a SELECT clause to the form SeaStar keys columns by.
 */
final class Selectables {

	private Selectables() {
		// hidden constructor
	}

	/**
	 * Resolves a selected identifier to its internal (stored) column name.
	 *
	 * <p>Unlike the {@code ColumnIdentifier} in a WHERE clause, a {@code Selectable.RawIdentifier}
	 * holds the identifier exactly as it was typed: {@code SELECT MyCol} keeps its capitals, and
	 * {@code toString()} returns them unchanged. Only {@code toFieldIdentifier()} applies CQL's
	 * folding rule - lower-case an unquoted identifier, preserve a quoted one - so it, not
	 * {@code toString()}, is what maps onto {@link CqlIdentifier#fromInternal(String)}.
	 */
	static CqlIdentifier toIdentifier(final Selectable.RawIdentifier identifier) {
		return CqlIdentifier.fromInternal(identifier.toFieldIdentifier().toString());
	}

}
