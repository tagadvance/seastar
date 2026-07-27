package com.tagadvance.seastar.handlers;

import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * A SELECT translated against the table it reads. What the statement says, with no parse tree left
 * in it; whether it is allowed to say it - DISTINCT on a non-key column, filtering without ALLOW
 * FILTERING - is the handler's business.
 *
 * @param target        the keyspace and table being read
 * @param selectors     the select clause, in the order it was written, or empty for
 *                      {@code SELECT *}
 * @param json          whether {@code SELECT JSON} was written, which replaces every result column
 *                      with the single {@code [json]} text column holding all of them
 * @param distinct      whether DISTINCT was written
 * @param allowFiltering whether ALLOW FILTERING was written
 * @param restrictions  the WHERE clause; empty when there is none
 * @param orderBy       the ORDER BY clause, in the order it was written; empty when there is none
 * @param limit         the LIMIT, or null when there is none
 */
record Query(Target target, List<Selector> selectors, boolean json, boolean distinct,
			 boolean allowFiltering, List<Restriction> restrictions, List<Sort> orderBy,
			 @Nullable Integer limit) {

	/**
	 * Whether the statement was written as {@code SELECT *}, which returns the table's columns as
	 * they stand rather than a fixed list.
	 */
	boolean isWildcard() {
		return selectors.isEmpty();
	}

	/**
	 * Whether any selector aggregates, which makes the whole result one row.
	 */
	boolean isAggregate() {
		return selectors.stream().anyMatch(Selector::isAggregate);
	}

}
