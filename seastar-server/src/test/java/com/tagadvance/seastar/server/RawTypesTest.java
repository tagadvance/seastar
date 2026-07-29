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
 * result metadata is what these assert.
 */
class RawTypesTest {

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
		final var read = DataTypeHelper.fromProtocolSpec(RawTypes.of(type), AttachmentPoint.NONE);

		assertEquals(type, read);
	}

	@Test
	@DisplayName("a primitive is written as its protocol code")
	void testPrimitive() {
		assertEquals(RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT),
			RawTypes.of(DataTypes.INT));
		assertEquals(RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR),
			RawTypes.of(DataTypes.TEXT));
	}

	@Test
	@DisplayName("duration travels as a custom type, because it is not a primitive until v5")
	void testDuration() {
		final var custom = assertInstanceOf(RawType.RawCustom.class,
			RawTypes.of(DataTypes.DURATION));

		assertEquals("org.apache.cassandra.db.marshal.DurationType", custom.className);
	}

	@Test
	@DisplayName("a collection carries its element types, recursively")
	void testCollections() {
		assertEquals(new RawType.RawList(RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT)),
			RawTypes.of(DataTypes.listOf(DataTypes.INT)));
		assertEquals(new RawType.RawSet(RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR)),
			RawTypes.of(DataTypes.setOf(DataTypes.TEXT)));
		assertEquals(new RawType.RawMap(RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR),
				new RawType.RawList(RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT))),
			RawTypes.of(DataTypes.mapOf(DataTypes.TEXT, DataTypes.listOf(DataTypes.INT))));
	}

	@Test
	@DisplayName("frozen is a schema concept and does not reach the wire")
	void testFrozenIsNotEncoded() {
		assertEquals(RawTypes.of(DataTypes.listOf(DataTypes.INT)),
			RawTypes.of(DataTypes.frozenListOf(DataTypes.INT)));
		assertEquals(RawTypes.of(DataTypes.setOf(DataTypes.TEXT)),
			RawTypes.of(DataTypes.frozenSetOf(DataTypes.TEXT)));
	}

	@Test
	@DisplayName("a tuple carries its component types in order")
	void testTuple() {
		final var tuple = assertInstanceOf(RawType.RawTuple.class,
			RawTypes.of(DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT)));

		assertEquals(List.of(RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT),
			RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR)), tuple.fieldTypes);
	}

	@Test
	@DisplayName("a user-defined type carries its whole field list, in the declared order")
	void testUserDefinedType() {
		final var udt = assertInstanceOf(RawType.RawUdt.class, RawTypes.of(ADDRESS));

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
			RawTypes.of(DataTypes.vectorOf(DataTypes.FLOAT, 3)));

		assertEquals("org.apache.cassandra.db.marshal.VectorType("
			+ "org.apache.cassandra.db.marshal.FloatType, 3)", custom.className);
	}

	@Test
	@DisplayName("a vector of something with no marshaller name is refused by name")
	void testVectorOfAnUnnameableElement() {
		final var vector = DataTypes.vectorOf(DataTypes.tupleOf(DataTypes.INT), 2);
		final var thrown = assertThrows(UnsupportedOperationException.class,
			() -> RawTypes.of(vector));

		assertTrue(thrown.getMessage().contains("tuple<int>"), thrown.getMessage());
	}

}
