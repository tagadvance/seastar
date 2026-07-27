package com.tagadvance.seastar.handlers;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.statements.schema.KeyspaceAttributes;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;

/**
 * The replication options a {@code CREATE} or {@code ALTER KEYSPACE} statement names, in the form a
 * live cluster reports them.
 */
final class Replication {

	private static final String LOCATOR_PACKAGE = "org.apache.cassandra.locator.";

	private Replication() {
		// hidden constructor
	}

	/**
	 * The replication the statement asked for, or empty when it named none - which on a
	 * {@code CREATE} means the default and on an {@code ALTER} means leave it alone.
	 */
	@SuppressWarnings("unchecked")
	static Optional<Map<String, String>> of(final KeyspaceAttributes attrs) {
		final var properties = FieldBindings.PROPERTY_DEFINITIONS_PROPERTIES.require(attrs);

		return Optional.ofNullable(
				(Map<String, String>) properties.get(KeyspaceParams.Option.REPLICATION.toString()))
			.map(Replication::qualifyStrategyClass);
	}

	/**
	 * Cassandra records the fully qualified strategy class in {@code system_schema.keyspaces}, so
	 * {@code KeyspaceMetadata#getReplication()} on a live cluster reports
	 * {@code org.apache.cassandra.locator.SimpleStrategy} even though the CQL only said
	 * {@code SimpleStrategy}. Apply the same expansion rule
	 * ({@code AbstractReplicationStrategy#getClass(String)}) so the metadata matches.
	 */
	private static Map<String, String> qualifyStrategyClass(final Map<String, String> replication) {
		return replication.entrySet()
			.stream()
			.collect(Collectors.toUnmodifiableMap(Entry::getKey,
				entry -> ReplicationParams.CLASS.equals(entry.getKey()) ? qualify(entry.getValue())
					: entry.getValue()));
	}

	private static String qualify(final String strategyClass) {
		return strategyClass.contains(".") ? strategyClass : LOCATOR_PACKAGE + strategyClass;
	}

}
