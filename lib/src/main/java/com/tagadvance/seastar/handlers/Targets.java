package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarKeyspace;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.statements.QualifiedStatement;
import org.jspecify.annotations.Nullable;

/**
 * Resolves the keyspace and table a statement names: the keyspace written into the statement, else
 * the session keyspace, then the keyspace and the table it holds.
 *
 * <p>Every handler that addresses a table goes through here, so the three failures - no keyspace at
 * all, an unknown keyspace, an unknown table - are reported identically wherever they happen.
 *
 * <p>The wording is {@code Schema}'s, verified against a live cluster: a statement that names a
 * keyspace is validated there, so it reports {@code keyspace x does not exist}. {@code ClientState}
 * carries a differently worded {@code Keyspace 'x' does not exist} for the keyspace a session is
 * switched to, which is why {@code UseKeyspaceHandler} keeps that form.
 */
final class Targets {

	private static final String NO_KEYSPACE =
		"No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename";

	private Targets() {
		// hidden constructor
	}

	/**
	 * Resolves the target of a statement written as {@code [keyspace.]table}.
	 */
	static Target require(final SeaStarDriverContext context,
		final Supplier<Optional<CqlIdentifier>> sessionKeyspace, final QualifiedStatement raw,
		final Node coordinator) {
		return require(context, sessionKeyspace, keyspaceOf(raw), raw.name(), coordinator);
	}

	/**
	 * Resolves the target a schema statement names, which carries its {@link QualifiedName} rather
	 * than being one.
	 */
	static Target require(final SeaStarDriverContext context,
		final Supplier<Optional<CqlIdentifier>> sessionKeyspace, final QualifiedName name,
		final Node coordinator) {
		return require(context, sessionKeyspace, keyspaceOf(name), name.getName(), coordinator);
	}

	private static Target require(final SeaStarDriverContext context,
		final Supplier<Optional<CqlIdentifier>> sessionKeyspace,
		final @Nullable String statementKeyspace, final String table, final Node coordinator) {
		final var keyspace = requireKeyspace(context, sessionKeyspace, statementKeyspace, coordinator);
		final var seaStarTable = keyspace.getSeaStarTable(CqlIdentifier.fromInternal(table))
			.orElseThrow(() -> new InvalidQueryException(coordinator,
				"table %s does not exist".formatted(table)));

		return new Target(keyspace, seaStarTable);
	}

	/**
	 * Resolves the keyspace a statement operates in, for the statements that name no table.
	 */
	static SeaStarKeyspace requireKeyspace(final SeaStarDriverContext context,
		final Supplier<Optional<CqlIdentifier>> sessionKeyspace,
		final @Nullable String statementKeyspace, final Node coordinator) {
		final var name = requireKeyspaceName(sessionKeyspace, statementKeyspace, coordinator);

		return context.getSeaStarKeyspace(name)
			.orElseThrow(() -> new InvalidQueryException(coordinator,
				"keyspace %s does not exist".formatted(name.asInternal())));
	}

	/**
	 * Resolves the name of the keyspace a statement operates in, without requiring that it exists.
	 * For the handlers that report a missing keyspace as something other than a missing keyspace.
	 */
	static CqlIdentifier requireKeyspaceName(
		final Supplier<Optional<CqlIdentifier>> sessionKeyspace,
		final @Nullable String statementKeyspace, final Node coordinator) {
		return Optional.ofNullable(statementKeyspace)
			.map(CqlIdentifier::fromInternal)
			.or(sessionKeyspace)
			.orElseThrow(() -> new InvalidQueryException(coordinator, NO_KEYSPACE));
	}

	/**
	 * The keyspace a statement was written with, or {@code null} when it was left unqualified.
	 *
	 * <p>{@link QualifiedStatement#keyspace()} throws rather than returning null when the statement
	 * was never qualified and {@code setKeyspace} was never called, so the question has to be asked
	 * before the value is read.
	 */
	private static @Nullable String keyspaceOf(final QualifiedStatement raw) {
		return raw.isFullyQualified() ? raw.keyspace() : null;
	}

	private static @Nullable String keyspaceOf(final QualifiedName name) {
		return name.hasKeyspace() ? name.getKeyspace() : null;
	}

}
