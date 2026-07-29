package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import org.jspecify.annotations.Nullable;

/**
 * What a CQL statement is, in the only terms a server answering it over the wire has to care about:
 * whether it selects a keyspace, changes the schema, or does neither.
 *
 * <p>SeaStar itself has no use for this - a handler answers with an {@code AsyncResultSet} and the
 * in-process caller reads rows out of it. Cassandra's native protocol does not have that luxury: it
 * answers a {@code USE} with {@code SET_KEYSPACE} and a DDL statement with {@code SCHEMA_CHANGE},
 * and a driver reads both to decide what metadata to refresh. This is the smallest thing the core
 * can say that lets a server pick the right one, and it exists so that a server does not have to
 * read the parse tree itself - which would spread the {@code org.apache.cassandra} imports out of
 * this package.
 *
 * <p>Summarize <em>before</em> running the statement. {@code DROP INDEX} names only the index, so
 * the table whose metadata a driver has to refresh can only be found while the index is still
 * there.
 *
 * <p><strong>A DDL statement that changes nothing is still summarized as a schema change.</strong>
 * A real node answers {@code CREATE TABLE IF NOT EXISTS} on a table that already exists with
 * {@code VOID}, because it compares the schema before and after; that comparison is not something a
 * statement summary can do. Over-reporting costs a connected driver one redundant metadata refresh;
 * under-reporting would leave it holding stale metadata, so this errs in the direction that is only
 * ever wasteful.
 */
public sealed interface CqlStatementSummary {

	/**
	 * A statement that neither selects a keyspace nor changes the schema: {@code SELECT}, the
	 * modifications, {@code TRUNCATE}, {@code BATCH}. Whether it answers with rows or with nothing is
	 * a question for its result set, not for its text.
	 */
	record Result() implements CqlStatementSummary {

	}

	/**
	 * {@code USE}.
	 *
	 * @param keyspace the keyspace the statement selects, as written
	 */
	record KeyspaceSelected(String keyspace) implements CqlStatementSummary {

	}

	/**
	 * Any DDL statement.
	 *
	 * @param change   what happened to {@code object}
	 * @param target   what kind of thing {@code object} is
	 * @param keyspace the keyspace it lives in, resolved against the session keyspace where the
	 *                 statement left it out
	 * @param object   the table or type it names, {@code null} when the keyspace itself is the
	 *                 subject, and also {@code null} for a {@code DROP INDEX} naming an index that
	 *                 is not there to be found
	 */
	record SchemaChanged(Change change, Target target, String keyspace, @Nullable String object)
		implements CqlStatementSummary {

	}

	/** What a DDL statement did. */
	enum Change {
		CREATED, UPDATED, DROPPED
	}

	/**
	 * What a DDL statement did it to. There is no {@code INDEX}: a real node reports creating or
	 * dropping an index as an update to the table that owns it, because an index is not a schema
	 * object a driver tracks separately.
	 */
	enum Target {
		KEYSPACE, TABLE, TYPE
	}

	/**
	 * Summarizes {@code query} without running it.
	 *
	 * @param metadata        the cluster metadata, consulted only to find the table a
	 *                        {@code DROP INDEX} belongs to
	 * @param sessionKeyspace the keyspace an unqualified statement resolves against, or {@code null}
	 *                        if none is selected
	 * @param query           the CQL to summarize
	 *
	 * @return what the statement is
	 *
	 * @throws com.datastax.oss.driver.api.core.servererrors.SyntaxError if the query is not valid CQL
	 */
	static CqlStatementSummary of(final Metadata metadata,
		final @Nullable CqlIdentifier sessionKeyspace, final String query) {
		return StatementSummaries.of(metadata, sessionKeyspace, query);
	}

}
