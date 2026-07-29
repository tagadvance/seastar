package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarKeyspace;
import com.tagadvance.seastar.SeaStarUserDefinedType;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.schema.DropTypeStatement.Raw;

/**
 * Handles {@code DROP TYPE}. Mirrors {@code DropTypeStatement}: a type still named by another type
 * or by a table column is refused, because dropping it and recreating a different type under the
 * same name would leave the values already written unreadable. The two are reported in Cassandra's
 * order - user types first, then tables - and a nested reference counts, so a
 * {@code list<frozen<addr>>} column holds the type just as much as an {@code addr} column does.
 *
 * <p>A missing keyspace is reported as a missing type, which is what a live node does: it looks up
 * the type rather than the keyspace.
 */
@ThreadSafe
public class DropTypeHandler implements CqlHandler<Raw> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public DropTypeHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof Raw;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Raw raw, final Object... bindings) {
		final var node = executionInfo.getCoordinator();
		final var name = FieldBindings.DROP_TYPE_NAME.require(raw);
		final var ifExists = FieldBindings.DROP_TYPE_IF_EXISTS.require(raw);

		final CqlIdentifier keyspaceName;
		try {
			keyspaceName = Targets.requireKeyspaceName(getKeyspace, name.getKeyspace(), node);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		final var typeName = name.getStringTypeName();
		final var keyspace = context.getSeaStarKeyspace(keyspaceName).orElse(null);
		final var type = Optional.ofNullable(keyspace)
			.flatMap(ks -> ks.getSeaStarUserDefinedType(typeName))
			.orElse(null);
		if (type == null) {
			if (ifExists) {
				return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
			}

			return CompletableFuture.failedStage(new InvalidQueryException(node,
				"Type '%s.%s' doesn't exist".formatted(keyspaceName.asInternal(), typeName)));
		}

		final var qualified = "%s.%s".formatted(keyspaceName.asInternal(), typeName);
		final var types = referencingTypes(keyspace, type);
		if (!types.isEmpty()) {
			return CompletableFuture.failedStage(new InvalidQueryException(node,
				"Cannot drop user type '%s' as it is still used by user types %s".formatted(qualified,
					types)));
		}

		final var tables = referencingTables(keyspace, type);
		if (!tables.isEmpty()) {
			return CompletableFuture.failedStage(new InvalidQueryException(node,
				"Cannot drop user type '%s' as it is still used by tables %s".formatted(qualified,
					tables)));
		}

		keyspace.removeSeaStarUserDefinedType(CqlIdentifier.fromInternal(typeName));
		// Prepared statements whose variables name the type describe something that is now gone.
		SchemaChanges.typeDropped(context, type);

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

	private static String referencingTypes(final SeaStarKeyspace keyspace,
		final SeaStarUserDefinedType type) {
		return keyspace.getSeaStarUserDefinedTypes()
			.values()
			.stream()
			.filter(candidate -> !isSameType(type, candidate))
			.filter(candidate -> candidate.getFieldTypes()
				.stream()
				.anyMatch(field -> references(type, field)))
			.map(candidate -> candidate.getName().asInternal())
			.collect(Collectors.joining(", "));
	}

	private static String referencingTables(final SeaStarKeyspace keyspace,
		final SeaStarUserDefinedType type) {
		return keyspace.getSeaStarTables()
			.values()
			.stream()
			.filter(table -> table.getColumns()
				.values()
				.stream()
				.map(ColumnMetadata::getType)
				.anyMatch(column -> references(type, column)))
			.map(table -> table.getName().asInternal())
			.collect(Collectors.joining(", "));
	}

	/**
	 * Whether {@code candidate} is, or holds anywhere inside it, the type being dropped.
	 */
	private static boolean references(final UserDefinedType type, final DataType candidate) {
		if (candidate instanceof UserDefinedType udt) {
			return isSameType(type, udt) || udt.getFieldTypes()
				.stream()
				.anyMatch(field -> references(type, field));
		}
		if (candidate instanceof ListType list) {
			return references(type, list.getElementType());
		}
		if (candidate instanceof SetType set) {
			return references(type, set.getElementType());
		}
		if (candidate instanceof MapType map) {
			return references(type, map.getKeyType()) || references(type, map.getValueType());
		}
		if (candidate instanceof TupleType tuple) {
			return tuple.getComponentTypes().stream().anyMatch(field -> references(type, field));
		}
		if (candidate instanceof VectorType vector) {
			return references(type, vector.getElementType());
		}

		return false;
	}

	/**
	 * Compared by name rather than by {@code equals}: a reference carries its own frozen flag and a
	 * snapshot of the fields, so a column's copy of a type is never equal to the type itself.
	 */
	private static boolean isSameType(final UserDefinedType type, final UserDefinedType candidate) {
		return type.getKeyspace().equals(candidate.getKeyspace()) && type.getName()
			.equals(candidate.getName());
	}

}
