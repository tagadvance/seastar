package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * One element of an ORDER BY clause, resolved against the table it reads.
 *
 * <p>Only the column and the direction are translation's business. Whether ORDER BY may be written
 * at all - Cassandra allows it solely on the clustering columns of a single-partition query, in
 * their declared order, and only if every element agrees on reading that order forwards or
 * backwards - is a rule about the store, so it stays with the handler.
 *
 * @param column     the name of the column to order by
 * @param descending whether DESC was written; ASC is the default
 */
record Sort(CqlIdentifier column, boolean descending) {

}
