package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.Optional;
import java.util.stream.Stream;
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
		if (name != null) {
			// FIXME: handle frozen UDT
		}

		if (type instanceof Native n) {
			final var ordinal = n.ordinal();
			final var match = dataTypes().filter(dt -> dt.getProtocolCode() == ordinal).findFirst();
			if (match.isPresent()) {
				return match;
			}
		} else if (type.isCollection()) {
			System.gc();
		} else if (type.isUDT()) {
			System.gc();
		} else if (type.isVector()) {
			System.gc();
		}

		throw new IllegalStateException("Unsupported CQL3Type: %s".formatted(type));
	}

	private static Stream<DataType> dataTypes() {
		return Stream.of(DataTypes.ASCII, DataTypes.BIGINT, DataTypes.BLOB, DataTypes.BOOLEAN,
			DataTypes.COUNTER, DataTypes.DECIMAL, DataTypes.DOUBLE, DataTypes.FLOAT, DataTypes.INT,
			DataTypes.TIMESTAMP, DataTypes.UUID, DataTypes.VARINT, DataTypes.TIMEUUID,
			DataTypes.INET, DataTypes.DATE, DataTypes.TEXT, DataTypes.TIME, DataTypes.SMALLINT,
			DataTypes.TINYINT, DataTypes.DURATION);
	}

}
