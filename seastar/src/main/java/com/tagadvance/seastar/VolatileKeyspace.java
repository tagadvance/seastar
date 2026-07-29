package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;

/**
 * A keyspace, and the one lock everything inside it is guarded by.
 *
 * <p>There is no table lock and no row lock: {@link VolatileTable}, {@link VolatileColumn},
 * {@link VolatileRow} and {@link VolatileUserDefinedType} all hand back <em>this</em> lock, so a
 * statement that mutates a table holds one lock for the whole of it and a concurrent DDL statement
 * against the same keyspace cannot interleave. Two keyspaces do not contend at all. See the lock
 * hierarchy in {@code AGENTS.md}.
 */
@ThreadSafe
class VolatileKeyspace implements SeaStarKeyspace {

	/**
	 * Immutable, and shared with everything this keyspace holds.
	 */
	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	/**
	 * Immutable.
	 */
	private final SeaStarDriverContext context;
	/**
	 * Immutable.
	 */
	private final CqlIdentifier name;
	@GuardedBy("lock")
	private Map<String, String> replication;
	@GuardedBy("lock")
	private boolean durableWrites;
	@GuardedBy("lock")
	private final Map<CqlIdentifier, SeaStarUserDefinedType> userDefinedTypes;
	@GuardedBy("lock")
	private final Map<CqlIdentifier, SeaStarTable> tables;

	public VolatileKeyspace(final SeaStarDriverContext context, final CqlIdentifier name,
		final Map<String, String> replication, final boolean durableWrites) {
		this.context = requireNonNull(context, "context must not be null");
		this.name = requireNonNull(name, "name must not be null");
		this.replication = Map.copyOf(
			requireNonNull(replication, "replication must not be null"));
		this.durableWrites = durableWrites;
		this.userDefinedTypes = new LinkedHashMap<>();
		this.tables = new LinkedHashMap<>();
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
	public void alter(final Map<String, String> replication, final boolean durableWrites) {
		requireNonNull(replication, "replication must not be null");

		writeLock(() -> {
			this.replication = Map.copyOf(replication);
			this.durableWrites = durableWrites;
		});
	}

	@Override
	public Optional<SeaStarUserDefinedType> getSeaStarUserDefinedType(final CqlIdentifier id) {
		return readLockUnchecked(() -> Optional.ofNullable(userDefinedTypes.get(id)));
	}

	@Override
	public void putSeaStarUserDefinedType(final SeaStarUserDefinedType userDefinedType) {
		writeLock(() -> userDefinedTypes.put(userDefinedType.getName(), userDefinedType));
	}

	@Override
	public void removeSeaStarUserDefinedType(final CqlIdentifier id) {
		writeLock(() -> userDefinedTypes.remove(id));
	}

	/**
	 * A snapshot, taken under the read lock: a live view would keep mutating under a caller that is
	 * still walking it.
	 */
	@Override
	public Map<CqlIdentifier, SeaStarUserDefinedType> getSeaStarUserDefinedTypes() {
		return readLockUnchecked(() -> Map.copyOf(userDefinedTypes));
	}

	@Override
	public Optional<SeaStarTable> getSeaStarTable(final CqlIdentifier id) {
		return readLockUnchecked(() -> Optional.ofNullable(tables.get(id)));
	}

	@Override
	public void putSeaStarTable(final SeaStarTable table) {
		writeLock(() -> tables.put(table.getName(), table));
	}

	@Override
	public void removeSeaStarTable(final CqlIdentifier id) {
		writeLock(() -> tables.remove(id));
	}

	/**
	 * A snapshot, taken under the read lock.
	 *
	 * @see #getSeaStarUserDefinedTypes()
	 */
	@Override
	public Map<CqlIdentifier, SeaStarTable> getSeaStarTables() {
		return readLockUnchecked(() -> Map.copyOf(tables));
	}

	@Override
	@NonNull
	public CqlIdentifier getName() {
		return name;
	}

	@Override
	public boolean isDurableWrites() {
		return readLockUnchecked(() -> durableWrites);
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
		return readLockUnchecked(() -> replication);
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, TableMetadata> getTables() {
		return getSeaStarTables().entrySet()
			.stream()
			.collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	/**
	 * SeaStar does not support materialized views - {@code CREATE MATERIALIZED VIEW} is rejected -
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
		return getSeaStarUserDefinedTypes().entrySet()
			.stream()
			.collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	/**
	 * SeaStar does not support user-defined functions - {@code CREATE FUNCTION} is rejected - so a
	 * keyspace never holds one, and an empty map is the same answer a live cluster gives.
	 */
	@Override
	@NonNull
	public Map<FunctionSignature, FunctionMetadata> getFunctions() {
		return Map.of();
	}

	/**
	 * SeaStar does not support user-defined aggregates - {@code CREATE AGGREGATE} is rejected - so a
	 * keyspace never holds one, and an empty map is the same answer a live cluster gives.
	 */
	@Override
	@NonNull
	public Map<FunctionSignature, AggregateMetadata> getAggregates() {
		return Map.of();
	}

}
