package com.tagadvance.seastar.server;

import com.datastax.oss.driver.api.core.type.CustomType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.RawType;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Driver {@link DataType} to protocol {@link RawType}: what a column's type looks like in the
 * metadata of a result message.
 *
 * <p>Two things about the mapping are worth knowing before changing it, both captured from a
 * {@code cassandra:5.0.8} container rather than reasoned about:
 *
 * <ul>
 *   <li><strong>Frozen-ness is not on the wire.</strong> {@code frozen<list<int>>} and
 *       {@code list<int>} encode identically; frozen is a schema concept, and a driver reading a
 *       result set never learns which it was.</li>
 *   <li><strong>Two types travel as custom types at protocol v4.</strong> {@code duration} became a
 *       primitive only in v5, and a vector has no protocol id at all, so a real node sends both as
 *       a Cassandra marshaller class name. The driver's {@code DataTypes#custom} names each of them
 *       explicitly on the way back in, which is the proof this is the shape it expects rather than
 *       a shape it merely tolerates.</li>
 * </ul>
 */
final class RawTypes {

	private static final String MARSHAL = "org.apache.cassandra.db.marshal.";

	/**
	 * The marshaller class name a Cassandra node writes for each primitive, which is needed only to
	 * spell out a vector's element type. It is the inverse of the driver's own
	 * {@code DataTypeClassNameParser.NATIVE_TYPES_BY_CLASS_NAME}, so anything absent here is a type
	 * that parser would not read back.
	 */
	private static final Map<Integer, String> MARSHALLERS = Map.ofEntries(
		Map.entry(ProtocolConstants.DataType.ASCII, MARSHAL + "AsciiType"),
		Map.entry(ProtocolConstants.DataType.BIGINT, MARSHAL + "LongType"),
		Map.entry(ProtocolConstants.DataType.BLOB, MARSHAL + "BytesType"),
		Map.entry(ProtocolConstants.DataType.BOOLEAN, MARSHAL + "BooleanType"),
		Map.entry(ProtocolConstants.DataType.COUNTER, MARSHAL + "CounterColumnType"),
		Map.entry(ProtocolConstants.DataType.DECIMAL, MARSHAL + "DecimalType"),
		Map.entry(ProtocolConstants.DataType.DOUBLE, MARSHAL + "DoubleType"),
		Map.entry(ProtocolConstants.DataType.DURATION, MARSHAL + "DurationType"),
		Map.entry(ProtocolConstants.DataType.FLOAT, MARSHAL + "FloatType"),
		Map.entry(ProtocolConstants.DataType.INET, MARSHAL + "InetAddressType"),
		Map.entry(ProtocolConstants.DataType.INT, MARSHAL + "Int32Type"),
		Map.entry(ProtocolConstants.DataType.SMALLINT, MARSHAL + "ShortType"),
		Map.entry(ProtocolConstants.DataType.TIME, MARSHAL + "TimeType"),
		Map.entry(ProtocolConstants.DataType.TIMESTAMP, MARSHAL + "TimestampType"),
		Map.entry(ProtocolConstants.DataType.TIMEUUID, MARSHAL + "TimeUUIDType"),
		Map.entry(ProtocolConstants.DataType.TINYINT, MARSHAL + "ByteType"),
		Map.entry(ProtocolConstants.DataType.UUID, MARSHAL + "UUIDType"),
		Map.entry(ProtocolConstants.DataType.VARCHAR, MARSHAL + "UTF8Type"),
		Map.entry(ProtocolConstants.DataType.VARINT, MARSHAL + "IntegerType"),
		Map.entry(ProtocolConstants.DataType.DATE, MARSHAL + "SimpleDateType"));

	private RawTypes() {

	}

	/**
	 * @param type the column type to describe
	 * @return the same type as the protocol writes it
	 * @throws UnsupportedOperationException if the type has no representation at protocol v4
	 */
	static RawType of(final DataType type) {
		// VectorType extends CustomType, so it has to be asked about first.
		if (type instanceof VectorType vector) {
			return new RawType.RawCustom(MARSHAL + "VectorType(" + marshaller(vector.getElementType())
				+ ", " + vector.getDimensions() + ")");
		}
		if (type instanceof ListType list) {
			return new RawType.RawList(of(list.getElementType()));
		}
		if (type instanceof SetType set) {
			return new RawType.RawSet(of(set.getElementType()));
		}
		if (type instanceof MapType map) {
			return new RawType.RawMap(of(map.getKeyType()), of(map.getValueType()));
		}
		if (type instanceof TupleType tuple) {
			return new RawType.RawTuple(
				tuple.getComponentTypes().stream().map(RawTypes::of).collect(Collectors.toList()));
		}
		if (type instanceof UserDefinedType udt) {
			return udt(udt);
		}
		if (type instanceof CustomType custom) {
			return new RawType.RawCustom(custom.getClassName());
		}
		if (type.getProtocolCode() == ProtocolConstants.DataType.DURATION) {
			return new RawType.RawCustom(MARSHAL + "DurationType");
		}

		final var primitive = RawType.PRIMITIVES.get(type.getProtocolCode());
		if (primitive == null) {
			throw new UnsupportedOperationException(
				"SeaStar's listener cannot describe the type " + type.asCql(true, false)
					+ " over protocol v4");
		}

		return primitive;
	}

	/**
	 * A user-defined type carries its whole field list on the wire, not just its name, because a
	 * result set is meant to be readable without the schema. The field order is the declared one and
	 * is load-bearing - the driver rebuilds the type from this map's iteration order - so the map has
	 * to keep it.
	 */
	private static RawType udt(final UserDefinedType type) {
		final var fields = new LinkedHashMap<String, RawType>();
		final var names = type.getFieldNames();
		final var types = type.getFieldTypes();
		for (int i = 0; i < names.size(); i++) {
			fields.put(names.get(i).asInternal(), of(types.get(i)));
		}
		final var keyspace = type.getKeyspace();

		return new RawType.RawUdt(keyspace == null ? "" : keyspace.asInternal(),
			type.getName().asInternal(), fields);
	}

	private static String marshaller(final DataType type) {
		final var name = MARSHALLERS.get(type.getProtocolCode());
		if (name == null) {
			throw new UnsupportedOperationException(
				"SeaStar's listener cannot describe a vector of " + type.asCql(true, false));
		}

		return name;
	}

}
