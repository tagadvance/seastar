package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarKeyspace;
import com.tagadvance.seastar.SeaStarUserDefinedType;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.cql3.statements.schema.AlterTypeStatement.Raw;

/**
 * Handles {@code ALTER TYPE}. Mirrors {@code AlterTypeStatement}: fields may be appended or renamed,
 * and altering the type of an existing field is rejected outright.
 */
@ThreadSafe
public class AlterTypeHandler implements CqlHandler<Raw> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public AlterTypeHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
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
		final var name = Reflections.getDeclaredField(raw, "name", UTName.class).orElseThrow();
		final var keyspaceName = Optional.ofNullable(name.getKeyspace())
			.or(() -> getKeyspace.get().map(CqlIdentifier::asInternal))
			.orElse(null);
		if (keyspaceName == null) {
			return CompletableFuture.failedStage(new InvalidQueryException(node,
				"No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename"));
		}

		final var typeName = name.getStringTypeName();
		final var keyspace = context.getSeaStarKeyspace(keyspaceName).orElse(null);
		final var udt = Optional.ofNullable(keyspace)
			.flatMap(ks -> ks.getSeaStarUserDefinedType(typeName))
			.orElse(null);
		if (udt == null) {
			// A missing keyspace is reported the same way, matching AlterTypeStatement.apply.
			final var ifExists = Reflections.getDeclaredField(raw, "ifExists", Boolean.class)
				.orElse(false);
			if (ifExists) {
				return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
			}

			return CompletableFuture.failedStage(new InvalidQueryException(node,
				"Type %s.%s doesn't exist".formatted(keyspaceName, typeName)));
		}

		final var kind = Reflections.getDeclaredField(raw, "kind", Enum.class)
			.map(Enum::name)
			.orElseThrow();
		try {
			return CompletableFuture.completedStage(switch (kind) {
				case "ADD_FIELD" -> addField(executionInfo, raw, keyspace, udt);
				case "RENAME_FIELDS" -> renameFields(executionInfo, raw, udt);
				default -> throw new InvalidQueryException(node,
					"Altering field types is no longer supported");
			});
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}
	}

	private AsyncResultSet addField(final ExecutionInfo executionInfo, final Raw raw,
		final SeaStarKeyspace keyspace, final SeaStarUserDefinedType udt) {
		final var node = executionInfo.getCoordinator();
		final var fieldName = identifier(
			Reflections.getDeclaredField(raw, "newFieldName", FieldIdentifier.class).orElseThrow());
		final var ifFieldNotExists = Reflections.getDeclaredField(raw, "ifFieldNotExists",
			Boolean.class).orElse(false);
		final var rawType = Reflections.getDeclaredField(raw, "newFieldType", Object.class)
			.map(SeaStarRawType::from)
			.orElseThrow();

		return udt.writeLockUnchecked(() -> {
			if (udt.firstIndexOf(fieldName) >= 0) {
				if (ifFieldNotExists) {
					return newAsyncResultSet(executionInfo);
				}

				throw new InvalidQueryException(node,
					"Cannot add field %s to type %s: a field with name %s already exists".formatted(
						fieldName.asInternal(), udt.getName().asInternal(), fieldName.asInternal()));
			}

			final var dataType = rawType.toDataType(keyspace, node)
				.orElseThrow(() -> new InvalidQueryException(node,
					"Unknown type for field '%s'".formatted(fieldName.asInternal())));
			udt.addField(fieldName, dataType);

			return newAsyncResultSet(executionInfo);
		});
	}

	@SuppressWarnings("unchecked")
	private AsyncResultSet renameFields(final ExecutionInfo executionInfo, final Raw raw,
		final SeaStarUserDefinedType udt) {
		final var node = executionInfo.getCoordinator();
		final Map<Object, Object> renamedFields = Reflections.getDeclaredField(raw, "renamedFields",
			Map.class).orElseThrow();
		final var ifFieldExists = Reflections.getDeclaredField(raw, "ifFieldExists", Boolean.class)
			.orElse(false);

		return udt.writeLockUnchecked(() -> {
			final Map<CqlIdentifier, CqlIdentifier> renames = new LinkedHashMap<>();
			renamedFields.forEach((from, to) -> {
				final var current = identifier(from);
				if (udt.firstIndexOf(current) < 0) {
					if (!ifFieldExists) {
						throw new InvalidQueryException(node,
							"Unkown field %s in user type %s".formatted(current.asInternal(),
								udt.getName().asInternal()));
					}
				} else {
					renames.put(current, identifier(to));
				}
			});

			final var renamed = udt.getFieldNames()
				.stream()
				.map(field -> renames.getOrDefault(field, field))
				.toList();
			renamed.stream()
				.filter(field -> Collections.frequency(renamed, field) > 1)
				.findFirst()
				.ifPresent(duplicate -> {
					throw new InvalidQueryException(node,
						"Duplicate field name %s in type %s".formatted(duplicate.asInternal(),
							udt.getName().asInternal()));
				});

			udt.renameFields(renames);

			return newAsyncResultSet(executionInfo);
		});
	}

	private static CqlIdentifier identifier(final Object fieldIdentifier) {
		return CqlIdentifier.fromInternal(fieldIdentifier.toString());
	}

}
