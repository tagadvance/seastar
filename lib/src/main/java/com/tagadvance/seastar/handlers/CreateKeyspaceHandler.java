package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement.Raw;
import org.apache.cassandra.cql3.statements.schema.KeyspaceAttributes;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;

public class CreateKeyspaceHandler implements CqlHandler<CreateKeyspaceStatement.Raw> {

	private static final String LOCATOR_PACKAGE = "org.apache.cassandra.locator.";

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof CreateKeyspaceStatement.Raw;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Raw raw, final Object... bindings) {
		final var name = raw.keyspaceName;
		final var ifNotExists = FieldBindings.CREATE_KEYSPACE_IF_NOT_EXISTS.require(raw);
		final var optionalKeyspace = context.getSeaStarKeyspace(name);
		if (optionalKeyspace.isPresent()) {
			if (ifNotExists) {
				LOG.debug("Keyspace {} already exists, skipping creation", name);
			} else {
				return CompletableFuture.failedStage(
					new AlreadyExistsException(executionInfo.getCoordinator(), name, null));
			}
		} else {
			final var attrs = FieldBindings.CREATE_KEYSPACE_ATTRIBUTES.require(raw);
			final var durableWrites = attrs.getBoolean(
				KeyspaceParams.Option.DURABLE_WRITES.toString(),
				KeyspaceParams.DEFAULT_DURABLE_WRITES);

			context.newSeaStarKeyspace(CqlIdentifier.fromInternal(name), replication(attrs),
				durableWrites);
		}

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, String> replication(final KeyspaceAttributes attrs) {
		final var properties = FieldBindings.PROPERTY_DEFINITIONS_PROPERTIES.require(attrs);

		return Optional.ofNullable(
				(Map<String, String>) properties.get(KeyspaceParams.Option.REPLICATION.toString()))
			.map(CreateKeyspaceHandler::qualifyStrategyClass)
			.orElse(SeaStarDriverContext.DEFAULT_REPLICATION);
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
