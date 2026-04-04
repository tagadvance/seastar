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
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;

@ThreadSafe
public class VolatileKeyspace implements SeaStarKeyspace {

	private final SeaStarDriverContext context;
	private final CqlIdentifier name;
	private final Map<CqlIdentifier, SeaStarUserDefinedType> userDefinedTypes;
	private final Map<CqlIdentifier, SeaStarTable> tables;

	public VolatileKeyspace(final SeaStarDriverContext context, final CqlIdentifier name) {
		this.context = requireNonNull(context, "context must not be null");
		this.name = requireNonNull(name, "name must not be null");
		this.userDefinedTypes = new ConcurrentHashMap<>();
		this.tables = new ConcurrentHashMap<>();
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
	public void putSeaStarTable(final CqlIdentifier id, final SeaStarTable Table) {
		tables.put(id, Table);
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
		return false;
	}

	@Override
	public boolean isVirtual() {
		return false;
	}

	@Override
	@NonNull
	public Map<String, String> getReplication() {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, TableMetadata> getTables() {
		return getSeaStarTables().entrySet()
			.stream()
			.collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, ViewMetadata> getViews() {
		throw new UnsupportedOperationException("Views are not supported in SeaStarKeyspace");
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, UserDefinedType> getUserDefinedTypes() {
		return userDefinedTypes.entrySet()
			.stream()
			.collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	@Override
	@NonNull
	public Map<FunctionSignature, FunctionMetadata> getFunctions() {
		throw new UnsupportedOperationException("Functions are not supported in SeaStarKeyspace");
	}

	@Override
	@NonNull
	public Map<FunctionSignature, AggregateMetadata> getAggregates() {
		throw new UnsupportedOperationException("Aggregates are not supported in SeaStarKeyspace");
	}

}
