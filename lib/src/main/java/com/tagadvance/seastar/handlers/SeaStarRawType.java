package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.Optional;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQL3Type.Native;
import org.apache.cassandra.cql3.UTName;

record SeaStarRawType(UTName name, CQL3Type type, boolean isFrozen) {

	public static SeaStarRawType from(final Object o) {
		final var name = Reflections.getDeclaredField(o, "name", UTName.class).orElse(null);
		final var type = Reflections.getDeclaredField(o, "type", CQL3Type.class).orElse(null);
		final var frozen = Reflections.getDeclaredField(o, "frozen", boolean.class).orElse(false);

		return new SeaStarRawType(name, type, frozen);
	}

	public Optional<DataType> toDataType() {
		// FIXME: collections, UDTs, tuples, and vectors are not yet supported (returns empty).
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

}
