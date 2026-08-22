package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.api.core.session.Request;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.AuthenticationStatement;
import org.apache.cassandra.cql3.statements.AuthorizationStatement;
import org.apache.cassandra.cql3.statements.DescribeStatement;
import org.apache.cassandra.cql3.statements.schema.AlterViewStatement;
import org.apache.cassandra.cql3.statements.schema.CreateAggregateStatement;
import org.apache.cassandra.cql3.statements.schema.CreateFunctionStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTriggerStatement;
import org.apache.cassandra.cql3.statements.schema.CreateViewStatement;
import org.apache.cassandra.cql3.statements.schema.DropAggregateStatement;
import org.apache.cassandra.cql3.statements.schema.DropFunctionStatement;
import org.apache.cassandra.cql3.statements.schema.DropTriggerStatement;
import org.apache.cassandra.cql3.statements.schema.DropViewStatement;
import org.jspecify.annotations.Nullable;

/**
 * The answer SeaStar gives for a CQL statement it has no handler for: a driver exception naming the
 * feature and quoting the query, so that "SeaStar does not do this" cannot be mistaken for "your
 * CQL is wrong".
 *
 * <p>{@link #REJECTED} names the features SeaStar deliberately does not implement. Every other
 * unhandled statement falls through to a name derived from its parse-tree class, which is what a
 * cassandra-all upgrade that adds a statement type would hit; deriving the name keeps the internal
 * class out of the message either way.
 *
 * <p>The exception type is {@link InvalidQueryException} for nearly all of them, which is what a
 * live cluster answers when a feature is switched off rather than missing - {@code CREATE
 * MATERIALIZED VIEW} on a default 5.0 node gives {@code Materialized views are disabled. Enable in
 * cassandra.yaml to use.} and {@code CREATE FUNCTION} gives {@code User-defined functions are
 * disabled in cassandra.yaml}, both {@code InvalidQueryException}. SeaStar's reason differs - the
 * feature is not built rather than turned off - so the message says so and the type matches. The
 * auth statements are the exception: a default node - which also has no auth configured - answers
 * those with {@link UnauthorizedException}, so SeaStar does too.
 */
final class UnsupportedStatements {

	/**
	 * The statement families SeaStar rejects on purpose, and the feature each one names. Matched with
	 * {@code isInstance}, so a base class covers a family: {@code DescribeStatement}'s variants are
	 * anonymous subclasses, and every {@code GRANT}/{@code REVOKE}/{@code LIST}/{@code ROLE} statement
	 * descends from one of the two auth bases.
	 */
	private static final Map<Class<?>, String> REJECTED = Map.ofEntries(
		Map.entry(CreateViewStatement.Raw.class, "materialized views"),
		Map.entry(AlterViewStatement.Raw.class, "materialized views"),
		Map.entry(DropViewStatement.Raw.class, "materialized views"),
		Map.entry(CreateFunctionStatement.Raw.class, "user-defined functions"),
		Map.entry(DropFunctionStatement.Raw.class, "user-defined functions"),
		Map.entry(CreateAggregateStatement.Raw.class, "user-defined aggregates"),
		Map.entry(DropAggregateStatement.Raw.class, "user-defined aggregates"),
		Map.entry(CreateTriggerStatement.Raw.class, "triggers"),
		Map.entry(DropTriggerStatement.Raw.class, "triggers"),
		Map.entry(DescribeStatement.class, "DESCRIBE"),
		Map.entry(AuthenticationStatement.class, "roles and permissions"),
		Map.entry(AuthorizationStatement.class, "roles and permissions"));

	private UnsupportedStatements() {
		// hidden constructor
	}

	/**
	 * The failure for a statement no handler claimed.
	 */
	static CoordinatorException failure(final ExecutionInfo executionInfo,
		final CQLStatement.Raw raw) {
		final var feature = feature(raw);
		final var query = queryOf(executionInfo.getRequest());
		final var message = query == null ? "SeaStar does not support %s".formatted(feature)
			: "SeaStar does not support %s. The statement was: %s".formatted(feature, query);

		return isAuth(raw) ? new UnauthorizedException(executionInfo.getCoordinator(), message)
			: new InvalidQueryException(executionInfo.getCoordinator(), message);
	}

	private static boolean isAuth(final CQLStatement.Raw raw) {
		return raw instanceof AuthenticationStatement || raw instanceof AuthorizationStatement;
	}

	private static String feature(final CQLStatement.Raw raw) {
		return REJECTED.entrySet()
			.stream()
			.filter(entry -> entry.getKey().isInstance(raw))
			.map(Entry::getValue)
			.findFirst()
			.orElseGet(() -> statementName(raw.getClass()));
	}

	/**
	 * The CQL name of a statement, read off its parse-tree class rather than printed from it:
	 * {@code AlterTableStatement$Raw} is reported as {@code ALTER TABLE}. An anonymous statement class
	 * carries no simple name of its own, so the enclosing class is what gets named.
	 */
	private static String statementName(final Class<?> type) {
		var outer = type;
		while (outer.getEnclosingClass() != null) {
			outer = outer.getEnclosingClass();
		}

		final var name = outer.getSimpleName()
			.replaceFirst("Statement$", "")
			.replaceAll("(?<=[a-z0-9])(?=[A-Z])", " ")
			.toUpperCase(Locale.ROOT);

		return name.isEmpty() ? "this statement" : name;
	}

	/**
	 * The CQL a statement was written as, or {@code null} for a batch, which is a list of queries
	 * rather than one.
	 */
	private static @Nullable String queryOf(final Request request) {
		if (request instanceof SimpleStatement simple) {
			return simple.getQuery();
		}
		if (request instanceof BoundStatement bound) {
			return bound.getPreparedStatement().getQuery();
		}

		return null;
	}

}
