package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;

/**
 * The one place SeaStar turns a CQL string into a {@link CQLStatement.Raw} parse tree.
 *
 * <p>It calls {@link CQLFragmentParser} rather than {@link QueryProcessor#parseStatement(String)},
 * which is the same call plus exception translation. Going through {@code QueryProcessor} runs its
 * static initializer, which reads {@code DatabaseDescriptor}, builds a Caffeine prepared-statement
 * cache SeaStar never uses and schedules a recurring eviction warning that leaves a Cassandra
 * scheduler thread alive in every consumer's JVM. Measured at roughly 75 ms of a 743 ms cold start;
 * see benchmarks.md.
 *
 * <p>The translation is SeaStar's own, because {@code parseStatement} reports a failed parse as
 * Cassandra's server-side {@link SyntaxException} while a live cluster reports it to a client as the
 * driver's {@link SyntaxError}. Mapping here is what makes the two agree.
 */
public final class CqlParsers {

	private CqlParsers() {
		// hidden constructor
	}

	/**
	 * Parses {@code query}, reporting a failure the way a live cluster reports it to a driver.
	 *
	 * @param coordinator the node to attribute the failure to
	 * @param query       the CQL to parse
	 *
	 * @return the parse tree
	 *
	 * @throws SyntaxError          if the query is not valid CQL
	 * @throws InvalidQueryException if the parser rejects the query for any other reason
	 */
	public static CQLStatement.Raw parse(final Node coordinator, final String query) {
		try {
			return CQLFragmentParser.parseAnyUnhandled(CqlParser::query, query);
		} catch (final SyntaxException e) {
			throw new SyntaxError(coordinator, e.getMessage());
		} catch (final RequestValidationException e) {
			throw new InvalidQueryException(coordinator, e.getMessage());
		} catch (final RecognitionException e) {
			throw new SyntaxError(coordinator,
				"Invalid or malformed CQL query string: " + e.getMessage());
		} catch (final RuntimeException e) {
			throw new SyntaxError(coordinator,
				"Failed parsing statement: [%s] reason: %s %s".formatted(query,
					e.getClass().getSimpleName(), e.getMessage()));
		}
	}

}
