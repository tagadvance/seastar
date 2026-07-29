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
		final var name = FieldBindings.ALTER_TYPE_NAME.require(raw);
		final CqlIdentifier keyspaceName;
		try {
			keyspaceName = Targets.requireKeyspaceName(getKeyspace, name.getKeyspace(), node);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		final var typeName = name.getStringTypeName();
		final var keyspace = context.getSeaStarKeyspace(keyspaceName).orElse(null);
		final var udt = Optional.ofNullable(keyspace)
			.flatMap(ks -> ks.getSeaStarUserDefinedType(typeName))
			.orElse(null);
		if (udt == null) {
			// A missing keyspace is reported the same way, matching AlterTypeStatement.apply.
			final var ifExists = FieldBindings.ALTER_TYPE_IF_EXISTS.require(raw);
			if (ifExists) {
				return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
			}

			return CompletableFuture.failedStage(new InvalidQueryException(node,
				"Type %s.%s doesn't exist".formatted(keyspaceName.asInternal(), typeName)));
		}

		final var kind = FieldBindings.ALTER_TYPE_KIND.require(raw).name();
		try {
			final var result = switch (kind) {
				case "ADD_FIELD" -> addField(executionInfo, raw, keyspace, udt);
				case "RENAME_FIELDS" -> renameFields(executionInfo, raw, udt);
				default -> throw new InvalidQueryException(node,
					"Altering field types is no longer supported");
			};
			// Prepared statements whose variables name this UDT describe the fields it used to have.
			SchemaChanges.typeChanged(context, udt);

			return CompletableFuture.completedStage(result);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}
	}

	private AsyncResultSet addField(final ExecutionInfo executionInfo, final Raw raw,
		final SeaStarKeyspace keyspace, final SeaStarUserDefinedType udt) {
		final var node = executionInfo.getCoordinator();
		final var fieldName = identifier(FieldBindings.ALTER_TYPE_NEW_FIELD_NAME.require(raw));
		final var ifFieldNotExists = FieldBindings.ALTER_TYPE_IF_FIELD_NOT_EXISTS.require(raw);
		final var rawType = new SeaStarRawType(FieldBindings.ALTER_TYPE_NEW_FIELD_TYPE.require(raw));

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

	private AsyncResultSet renameFields(final ExecutionInfo executionInfo, final Raw raw,
		final SeaStarUserDefinedType udt) {
		final var node = executionInfo.getCoordinator();
		final var renamedFields = FieldBindings.ALTER_TYPE_RENAMED_FIELDS.require(raw);
		final var ifFieldExists = FieldBindings.ALTER_TYPE_IF_FIELD_EXISTS.require(raw);

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

	private static CqlIdentifier identifier(final FieldIdentifier fieldIdentifier) {
		return CqlIdentifier.fromInternal(fieldIdentifier.toString());
	}

}
