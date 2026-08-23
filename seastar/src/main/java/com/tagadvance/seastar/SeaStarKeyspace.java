package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.tagadvance.tools.SeaStarReadWriteLock;
import java.util.Map;
import java.util.Optional;

/**
 * A keyspace: its replication options, durable-writes flag, and the tables and user-defined types
 * it holds. Owns the one lock everything inside it - its tables, columns, rows and UDTs - shares;
 * see the lock hierarchy in {@code AGENTS.md}.
 *
 * <p>The {@code *SeaStar*} methods are SeaStar's own API - handlers, and tests populating the model
 * without CQL, go through them - and they hand back the mutable objects rather than the driver's
 * read-only views. Their {@code String} overloads resolve names with
 * {@link CqlIdentifier#fromInternal}: case-sensitive, no quote handling - unlike the driver's own
 * {@code getTable(String)} shortcuts, which go through {@code fromCql}.
 */
public interface SeaStarKeyspace extends SeaStarReadWriteLock, KeyspaceMetadata {

	/**
	 * The context this keyspace is registered with - the root of the lock hierarchy.
	 */
	SeaStarDriverContext context();

	/**
	 * The keyspace's name; the same value {@link #getName()} reports through the driver interface.
	 */
	CqlIdentifier name();

	/**
	 * Replaces the keyspace's replication options and its durable-writes flag. This is
	 * {@code ALTER KEYSPACE}, which replaces only the options the statement names, so the caller
	 * passes the values that survive alongside the ones that change.
	 */
	void alter(Map<String, String> replication, boolean durableWrites);

	/**
	 * Shortcut for {@link #getSeaStarUserDefinedType(CqlIdentifier)
	 * getSeaStarUserDefinedType(CqlIdentifier.fromInternal(name))}.
	 */
	default Optional<SeaStarUserDefinedType> getSeaStarUserDefinedType(final String name) {
		return getSeaStarUserDefinedType(CqlIdentifier.fromInternal(name));
	}

	/**
	 * The mutable user-defined type of that name, if this keyspace holds one.
	 */
	Optional<SeaStarUserDefinedType> getSeaStarUserDefinedType(CqlIdentifier id);

	/**
	 * Adds a user-defined type, replacing any existing type of the same name.
	 */
	void putSeaStarUserDefinedType(SeaStarUserDefinedType userDefinedType);

	/**
	 * Shortcut for {@link #removeSeaStarUserDefinedType(CqlIdentifier)
	 * removeSeaStarUserDefinedType(CqlIdentifier.fromInternal(name))}.
	 */
	default void removeSeaStarUserDefinedType(final String name) {
		removeSeaStarUserDefinedType(CqlIdentifier.fromInternal(name));
	}

	/**
	 * Removes the user-defined type of that name, if this keyspace holds one.
	 */
	void removeSeaStarUserDefinedType(CqlIdentifier id);

	/**
	 * Every user-defined type, as a snapshot taken under the read lock rather than a live view. The
	 * map is a copy; the types in it are the live objects.
	 */
	Map<CqlIdentifier, SeaStarUserDefinedType> getSeaStarUserDefinedTypes();

	/**
	 * Shortcut for {@link #getSeaStarTable(CqlIdentifier)
	 * getSeaStarTable(CqlIdentifier.fromInternal(name))}.
	 */
	default Optional<SeaStarTable> getSeaStarTable(final String name) {
		return getSeaStarTable(CqlIdentifier.fromInternal(name));
	}

	/**
	 * The mutable table of that name, if this keyspace holds one.
	 */
	Optional<SeaStarTable> getSeaStarTable(CqlIdentifier id);

	/**
	 * Shortcut for {@link #newSeaStarTable(CqlIdentifier)
	 * newSeaStarTable(CqlIdentifier.fromInternal(name))}.
	 */
	default SeaStarTable newSeaStarTable(final String name) {
		return newSeaStarTable(CqlIdentifier.fromInternal(name));
	}

	/**
	 * Creates an empty table, registers it under {@code id}, and returns it for the caller to shape
	 * with {@link SeaStarTable#addColumn(CqlIdentifier, com.datastax.oss.driver.api.core.type.DataType)
	 * addColumn}, {@link SeaStarTable#markPartitionKey markPartitionKey} and friends. This is how
	 * {@code CREATE TABLE} builds a table, and how a test populates the model without CQL.
	 */
	default SeaStarTable newSeaStarTable(final CqlIdentifier id) {
		final var table = new VolatileTable(context(), this, id);
		putSeaStarTable(table);

		return table;
	}

	/**
	 * Adds a table, replacing any existing table of the same name.
	 */
	void putSeaStarTable(SeaStarTable table);

	/**
	 * Shortcut for {@link #removeSeaStarTable(CqlIdentifier)
	 * removeSeaStarTable(CqlIdentifier.fromInternal(name))}.
	 */
	default void removeSeaStarTable(final String name) {
		removeSeaStarTable(CqlIdentifier.fromInternal(name));
	}

	/**
	 * Removes the table of that name, if this keyspace holds one. Removal alone does not mark the
	 * table dropped for anyone still holding a reference to it - {@code DROP TABLE} also calls
	 * {@link SeaStarTable#drop()}.
	 */
	void removeSeaStarTable(CqlIdentifier id);

	/**
	 * Every table, as a snapshot taken under the read lock rather than a live view. The map is a
	 * copy; the tables in it are the live objects.
	 */
	Map<CqlIdentifier, SeaStarTable> getSeaStarTables();

}
