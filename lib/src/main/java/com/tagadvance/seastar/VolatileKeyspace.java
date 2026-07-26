package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;

@ThreadSafe
public class VolatileKeyspace implements SeaStarKeyspace {

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private final SeaStarDriverContext context;
	private final CqlIdentifier name;
	private final Map<String, String> replication;
	private final boolean durableWrites;
	private final Map<CqlIdentifier, SeaStarUserDefinedType> userDefinedTypes;
	private final Map<CqlIdentifier, SeaStarTable> tables;

	public VolatileKeyspace(final SeaStarDriverContext context, final CqlIdentifier name,
		final Map<String, String> replication, final boolean durableWrites) {
		this.context = requireNonNull(context, "context must not be null");
		this.name = requireNonNull(name, "name must not be null");
		this.replication = Map.copyOf(
			requireNonNull(replication, "replication must not be null"));
		this.durableWrites = durableWrites;
		this.userDefinedTypes = new ConcurrentHashMap<>();
		this.tables = new ConcurrentHashMap<>();
	}

	@Override
	public ReadWriteLock lock() {
		return lock;
	}

	@Override
	public SeaStarDriverContext context() {
		return context;
	}

	@Override
	public CqlIdentifier name() {
		return name;
	}

	@Override
	public Optional<SeaStarUserDefinedType> getSeaStarUserDefinedType(final CqlIdentifier id) {
		return Optional.of(id).map(userDefinedTypes::get);
	}

	@Override
	public void putSeaStarUserDefinedType(final CqlIdentifier id,
		final SeaStarUserDefinedType object) {
		userDefinedTypes.put(id, object);
	}

	@Override
	public void removeSeaStarUserDefinedType(final CqlIdentifier id) {
		userDefinedTypes.remove(id);
	}

	@Override
	public Map<CqlIdentifier, SeaStarUserDefinedType> getSeaStarUserDefinedTypes() {
		return Collections.unmodifiableMap(userDefinedTypes);
	}

	@Override
	public Optional<SeaStarTable> getSeaStarTable(final CqlIdentifier id) {
		return Optional.of(id).map(tables::get);
	}

	@Override
	public void putSeaStarTable(final SeaStarTable table) {
		tables.put(table.getName(), table);
	}

	@Override
	public void removeSeaStarTable(final CqlIdentifier id) {
		tables.remove(id);
	}

	@Override
	public Map<CqlIdentifier, SeaStarTable> getSeaStarTables() {
		return Collections.unmodifiableMap(tables);
	}

	@Override
	@NonNull
	public CqlIdentifier getName() {
		return name;
	}

	@Override
	public boolean isDurableWrites() {
		return durableWrites;
	}

	@Override
	public boolean isVirtual() {
		return false;
	}

	/**
	 * The replication options this keyspace was created with, in the form a live cluster reports
	 * them: the {@code class} entry holds the fully qualified strategy class name, and every value is
	 * a string.
	 */
	@Override
	@NonNull
	public Map<String, String> getReplication() {
		return replication;
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, TableMetadata> getTables() {
		return getSeaStarTables().entrySet()
			.stream()
			.collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	/**
	 * SeaStar cannot create a materialized view - {@code CREATE MATERIALIZED VIEW} has no handler -
	 * so a keyspace never holds one. An empty map is what a live cluster returns for a keyspace
	 * without views, and it keeps metadata walkers such as {@link #describe(boolean)} working.
	 */
	@Override
	@NonNull
	public Map<CqlIdentifier, ViewMetadata> getViews() {
		return Map.of();
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, UserDefinedType> getUserDefinedTypes() {
		return userDefinedTypes.entrySet()
			.stream()
			.collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	/**
	 * SeaStar cannot create a user-defined function - {@code CREATE FUNCTION} has no handler - so a
	 * keyspace never holds one, and an empty map is the same answer a live cluster gives.
	 */
	@Override
	@NonNull
	public Map<FunctionSignature, FunctionMetadata> getFunctions() {
		return Map.of();
	}

	/**
	 * SeaStar cannot create a user-defined aggregate - {@code CREATE AGGREGATE} has no handler - so a
	 * keyspace never holds one, and an empty map is the same answer a live cluster gives.
	 */
	@Override
	@NonNull
	public Map<FunctionSignature, AggregateMetadata> getAggregates() {
		return Map.of();
	}

}
