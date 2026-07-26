package com.tagadvance.seastar.handlers;

import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * A SELECT translated against the table it reads. What the statement says, with no parse tree left
 * in it; whether it is allowed to say it - DISTINCT on a non-key column, filtering without ALLOW
 * FILTERING - is the handler's business.
 *
 * @param target        the keyspace and table being read
 * @param projection    the positions of the selected columns, in the order they were written, or
 *                      empty for {@code SELECT *}
 * @param distinct      whether DISTINCT was written
 * @param allowFiltering whether ALLOW FILTERING was written
 * @param restrictions  the WHERE clause; empty when there is none
 * @param limit         the LIMIT, or null when there is none
 */
record Query(Target target, List<Integer> projection, boolean distinct, boolean allowFiltering,
			 List<Restriction> restrictions, @Nullable Integer limit) {

}
