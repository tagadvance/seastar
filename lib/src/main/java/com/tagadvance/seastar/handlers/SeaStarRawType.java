package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.tagadvance.seastar.SeaStarKeyspace;
import java.util.Optional;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQL3Type.Native;
import org.apache.cassandra.db.marshal.CollectionType.Kind;

record SeaStarRawType(CQL3Type.Raw raw) {

	/**
	 * Resolves this parsed type to a driver {@link DataType}. UDT references are resolved against
	 * {@code keyspace}; an unknown UDT throws {@link InvalidQueryException} to mirror Cassandra.
	 * Returns empty only for a genuinely unrecognized native type.
	 */
	public Optional<DataType> toDataType(final SeaStarKeyspace keyspace, final Node coordinator) {
		if (raw.isVector()) {
			return toVector(keyspace, coordinator);
		}
		if (raw.isTuple()) {
			return toTuple(keyspace, coordinator);
		}
		if (raw.isUDT()) {
			return Optional.of(toUserDefined(keyspace, coordinator));
		}
		// CQL3Type.Raw publishes isUDT/isTuple/isVector but no isCollection, so a collection is
		// recognized by its implementation class rather than by probing for a field.
		if (FieldBindings.RAW_COLLECTION.isInstance(raw)) {
			return toCollection(FieldBindings.COLLECTION_KIND.require(raw), keyspace, coordinator);
		}

		return toNative();
	}

	/**
	 * Resolves this parsed type only if it is a native one, and returns empty for a UDT, tuple, vector
	 * or collection. For the places that name a type with no keyspace to resolve a named type against,
	 * such as a type cast written inside a term.
	 */
	public Optional<DataType> toNativeDataType() {
		return FieldBindings.RAW_TYPE.isInstance(raw) ? toNative() : Optional.empty();
	}

	private Optional<DataType> toNative() {
		return nativeDataType(FieldBindings.NATIVE_TYPE.require(raw));
	}

	/**
	 * The driver type an already-resolved native CQL type stands for, and empty for anything else. A
	 * cast in a SELECT clause names its target type this way rather than as a {@code Raw}.
	 */
	static Optional<DataType> nativeDataType(final CQL3Type type) {
		if (!(type instanceof Native n)) {
			return Optional.empty();
		}

		return Optional.ofNullable(switch (n.name()) {
			case "ASCII" -> DataTypes.ASCII;
			case "BIGINT" -> DataTypes.BIGINT;
			case "BLOB" -> DataTypes.BLOB;
			case "BOOLEAN" -> DataTypes.BOOLEAN;
			case "COUNTER" -> DataTypes.COUNTER;
			case "DATE" -> DataTypes.DATE;
			case "DECIMAL" -> DataTypes.DECIMAL;
			case "DOUBLE" -> DataTypes.DOUBLE;
			case "DURATION" -> DataTypes.DURATION;
			case "FLOAT" -> DataTypes.FLOAT;
			case "INET" -> DataTypes.INET;
			case "INT" -> DataTypes.INT;
			case "SMALLINT" -> DataTypes.SMALLINT;
			case "TEXT", "VARCHAR" -> DataTypes.TEXT;
			case "TIME" -> DataTypes.TIME;
			case "TIMESTAMP" -> DataTypes.TIMESTAMP;
			case "TIMEUUID" -> DataTypes.TIMEUUID;
			case "TINYINT" -> DataTypes.TINYINT;
			case "UUID" -> DataTypes.UUID;
			case "VARINT" -> DataTypes.VARINT;
			default -> null; // EMPTY and anything unrecognized
		});
	}

	private Optional<DataType> toCollection(final Kind kind, final SeaStarKeyspace keyspace,
		final Node coordinator) {
		final var frozen = raw.isFrozen();
		final var values = FieldBindings.COLLECTION_VALUES.require(raw);
		final var valueType = new SeaStarRawType(values).toDataType(keyspace, coordinator);
		if (valueType.isEmpty()) {
			return Optional.empty();
		}

		return switch (kind) {
			case LIST -> Optional.of(DataTypes.listOf(valueType.get(), frozen));
			case SET -> Optional.of(DataTypes.setOf(valueType.get(), frozen));
			// Only a map carries a key type; keys is null for a list or a set.
			case MAP -> new SeaStarRawType(FieldBindings.COLLECTION_KEYS.require(raw))
				.toDataType(keyspace, coordinator)
				.map(keyType -> DataTypes.mapOf(keyType, valueType.get(), frozen));
		};
	}

	private Optional<DataType> toTuple(final SeaStarKeyspace keyspace, final Node coordinator) {
		final var types = FieldBindings.TUPLE_TYPES.require(raw);
		final var componentTypes = new DataType[types.size()];
		for (int i = 0; i < types.size(); i++) {
			final var componentType = new SeaStarRawType(types.get(i)).toDataType(keyspace,
				coordinator);
			if (componentType.isEmpty()) {
				return Optional.empty();
			}
			componentTypes[i] = componentType.get();
		}

		return Optional.of(DataTypes.tupleOf(componentTypes));
	}

	private Optional<DataType> toVector(final SeaStarKeyspace keyspace, final Node coordinator) {
		final var element = FieldBindings.VECTOR_ELEMENT.require(raw);
		final var dimension = FieldBindings.VECTOR_DIMENSION.require(raw);

		return new SeaStarRawType(element).toDataType(keyspace, coordinator)
			.map(elementType -> DataTypes.vectorOf(elementType, dimension));
	}

	private DataType toUserDefined(final SeaStarKeyspace keyspace, final Node coordinator) {
		final var name = FieldBindings.USER_TYPE_NAME.require(raw);
		final var udtName = name.getStringTypeName();
		final var udt = keyspace.getSeaStarUserDefinedType(udtName)
			.orElseThrow(() -> new InvalidQueryException(coordinator, "Unknown type " + name));

		// Frozen is a property of the reference, not the stored type.
		return udt.copy(raw.isFrozen());
	}

}
