package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.type.DataTypeHelper;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.RawType;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * c_plan C3. Pure, total and the easiest thing in the wave to get subtly wrong in a way that shows
 * up for one column type in one user's schema, so every type SeaStar supports is named here.
 *
 * <p>The expectations come from a {@code cassandra:5.0.8} container: a table with one column of
 * every type was created and selected from over a raw v4 socket, and the {@code RawType} in the
 * result metadata is what these assert. Only {@code duration} differs at v5, where it stops being a
 * custom type and takes a protocol id of its own.
 */
class RawTypesTest {

	private static final int V4 = ProtocolConstants.Version.V4;
	private static final int V5 = ProtocolConstants.Version.V5;

	private static final DataType ADDRESS = new UserDefinedTypeBuilder(
		CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("address"))
		.withField(CqlIdentifier.fromInternal("street"), DataTypes.TEXT)
		.withField(CqlIdentifier.fromInternal("zip"), DataTypes.INT)
		.build();

	/**
	 * Every type a SeaStar column can have, so that adding one to the core without teaching this
	 * class about it fails here rather than in somebody's schema.
	 */
	static Stream<DataType> everySupportedType() {
		return Stream.of(DataTypes.ASCII, DataTypes.BIGINT, DataTypes.BLOB, DataTypes.BOOLEAN,
			DataTypes.COUNTER, DataTypes.DATE, DataTypes.DECIMAL, DataTypes.DOUBLE,
			DataTypes.DURATION, DataTypes.FLOAT, DataTypes.INET, DataTypes.INT, DataTypes.SMALLINT,
			DataTypes.TEXT, DataTypes.TIME, DataTypes.TIMESTAMP, DataTypes.TIMEUUID,
			DataTypes.TINYINT, DataTypes.UUID, DataTypes.VARINT,
			DataTypes.listOf(DataTypes.INT), DataTypes.setOf(DataTypes.TEXT),
			DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT),
			DataTypes.listOf(DataTypes.listOf(DataTypes.INT)),
			DataTypes.mapOf(DataTypes.TEXT, ADDRESS),
			DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT), ADDRESS,
			DataTypes.vectorOf(DataTypes.FLOAT, 3));
	}

	@ParameterizedTest
	@MethodSource("everySupportedType")
	@DisplayName("every supported type survives a round trip through the driver's own reader")
	void testRoundTrip(final DataType type) {
		// Both versions, because the whole point of describing a column is that the client rebuilds
		// the same type from it, and only one of the two paths is exercised by any one connection.
		assertEquals(type,
			DataTypeHelper.fromProtocolSpec(RawTypes.of(type, V4), AttachmentPoint.NONE));
		assertEquals(type,
			DataTypeHelper.fromProtocolSpec(RawTypes.of(type, V5), AttachmentPoint.NONE));
	}

	@Test
	@DisplayName("a primitive is written as its protocol code")
	void testPrimitive() {
		assertEquals(RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT),
			RawTypes.of(DataTypes.INT, V4));
		assertEquals(RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR),
			RawTypes.of(DataTypes.TEXT, V4));
	}

	@Test
	@DisplayName("duration travels as a custom type at v4, because it is not a primitive until v5")
	void testDuration() {
		final var custom = assertInstanceOf(RawType.RawCustom.class,
			RawTypes.of(DataTypes.DURATION, V4));

		assertEquals("org.apache.cassandra.db.marshal.DurationType", custom.className);
	}

	@Test
	@DisplayName("duration takes its own protocol code from v5 on")
	void testDurationAtV5() {
		assertEquals(RawType.PRIMITIVES.get(ProtocolConstants.DataType.DURATION),
			RawTypes.of(DataTypes.DURATION, V5));
	}

	@Test
	@DisplayName("a collection carries its element types, recursively")
	void testCollections() {
		assertEquals(new RawType.RawList(RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT)),
			RawTypes.of(DataTypes.listOf(DataTypes.INT), V4));
		assertEquals(new RawType.RawSet(RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR)),
			RawTypes.of(DataTypes.setOf(DataTypes.TEXT), V4));
		assertEquals(new RawType.RawMap(RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR),
				new RawType.RawList(RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT))),
			RawTypes.of(DataTypes.mapOf(DataTypes.TEXT, DataTypes.listOf(DataTypes.INT)), V4));
	}

	@Test
	@DisplayName("the version reaches a nested element type, not only the outermost one")
	void testVersionIsRecursive() {
		assertEquals(new RawType.RawList(RawType.PRIMITIVES.get(ProtocolConstants.DataType.DURATION)),
			RawTypes.of(DataTypes.listOf(DataTypes.DURATION), V5));
		assertInstanceOf(RawType.RawCustom.class,
			((RawType.RawList) RawTypes.of(DataTypes.listOf(DataTypes.DURATION), V4)).elementType);
	}

	@Test
	@DisplayName("frozen is a schema concept and does not reach the wire")
	void testFrozenIsNotEncoded() {
		assertEquals(RawTypes.of(DataTypes.listOf(DataTypes.INT), V4),
			RawTypes.of(DataTypes.frozenListOf(DataTypes.INT), V4));
		assertEquals(RawTypes.of(DataTypes.setOf(DataTypes.TEXT), V4),
			RawTypes.of(DataTypes.frozenSetOf(DataTypes.TEXT), V4));
	}

	@Test
	@DisplayName("a tuple carries its component types in order")
	void testTuple() {
		final var tuple = assertInstanceOf(RawType.RawTuple.class,
			RawTypes.of(DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT), V4));

		assertEquals(List.of(RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT),
			RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR)), tuple.fieldTypes);
	}

	@Test
	@DisplayName("a user-defined type carries its whole field list, in the declared order")
	void testUserDefinedType() {
		final var udt = assertInstanceOf(RawType.RawUdt.class, RawTypes.of(ADDRESS, V4));

		assertEquals("ks", udt.keyspace);
		assertEquals("address", udt.typeName);
		// A driver rebuilds the type from this map's iteration order, so the order is load-bearing.
		assertEquals(List.of("street", "zip"), List.copyOf(udt.fields.keySet()));
		assertEquals(RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR),
			udt.fields.get("street"));
	}

	@Test
	@DisplayName("a vector is a custom type naming its element marshaller and its dimension")
	void testVector() {
		final var custom = assertInstanceOf(RawType.RawCustom.class,
			RawTypes.of(DataTypes.vectorOf(DataTypes.FLOAT, 3), V4));

		assertEquals("org.apache.cassandra.db.marshal.VectorType("
			+ "org.apache.cassandra.db.marshal.FloatType, 3)", custom.className);
		// Still a class name at v5: native-protocol 1.5.2 has no RawVector to write instead.
		assertEquals(custom, RawTypes.of(DataTypes.vectorOf(DataTypes.FLOAT, 3), V5));
	}

	@Test
	@DisplayName("a vector of something with no marshaller name is refused by name")
	void testVectorOfAnUnnameableElement() {
		final var vector = DataTypes.vectorOf(DataTypes.tupleOf(DataTypes.INT), 2);
		final var thrown = assertThrows(UnsupportedOperationException.class,
			() -> RawTypes.of(vector, V4));

		assertTrue(thrown.getMessage().contains("tuple<int>"), thrown.getMessage());
	}

}
