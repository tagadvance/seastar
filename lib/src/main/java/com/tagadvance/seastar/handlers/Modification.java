package com.tagadvance.seastar.handlers;

import java.util.List;

/**
 * An INSERT, UPDATE or DELETE translated against the table it writes to. What the statement says,
 * with no parse tree left in it; what the statement is allowed to say is the handler's business.
 *
 * @param target       the keyspace and table being written
 * @param assignments  the columns the statement writes: an INSERT's values, an UPDATE's {@code SET}
 *                     items, or the columns a DELETE clears. Empty for a DELETE of whole rows
 * @param restrictions the WHERE clause; empty for an INSERT, which has none
 * @param conditions   the {@code IF} conditions of a lightweight transaction; empty when there are
 *                     none
 * @param ifExists     whether {@code IF EXISTS} was written
 * @param ifNotExists  whether {@code IF NOT EXISTS} was written
 */
record Modification(Target target, List<Assignment> assignments, List<Restriction> restrictions,
					List<Condition> conditions, boolean ifExists, boolean ifNotExists) {

}
