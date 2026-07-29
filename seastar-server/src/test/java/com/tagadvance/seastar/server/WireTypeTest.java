package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.type.DataTypeHelper;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Every CQL type SeaStar supports, written with a literal and read back off the wire.
 *
 * <p>This is the test that would catch C2 and C3 disagreeing. Each column is checked twice: the
 * {@code RawType} in the result metadata is fed back through the driver's own
 * {@code DataTypeHelper}, which is what a connecting driver uses, and must produce the type the
 * model holds; and the value bytes are decoded with the codec for that type and must be the value
 * that was written. A column whose type reads back wrong but whose bytes happen to parse would pass
 * only the second check.
 */
class WireTypeTest {

	private static final int V4 = ProtocolConstants.Version.V4;
	private static final UUID ID = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
	private static final UUID TIME_ID = UUID.fromString("8ac6d160-8b0d-11f1-b7c4-0242ac110002");

	private final AtomicInteger streamIds = new AtomicInteger();

	private SeaStarCqlSession session;
	private SeaStarProtocolServer server;
	private WireClient client;

	@BeforeEach
	void setUp() throws IOException {
		session = SeaStarCqlSession.builder().build();
		server = SeaStarProtocolServer.builder().session(session).build().start();
		client = new WireClient(server.port());
		client.send(V4, streamIds.incrementAndGet(), new Startup());
		send("CREATE KEYSPACE ks WITH replication = "
			+ "{'class':'SimpleStrategy','replication_factor':1}");
		send("CREATE TYPE ks.address (street text, zip int)");
		send("""
			CREATE TABLE ks.t (
			  id int PRIMARY KEY,
			  c_ascii ascii, c_bigint bigint, c_blob blob, c_boolean boolean,
			  c_date date, c_decimal decimal, c_double double, c_duration duration,
			  c_float float, c_inet inet, c_int int, c_smallint smallint,
			  c_text text, c_time time, c_timestamp timestamp, c_timeuuid timeuuid,
			  c_tinyint tinyint, c_uuid uuid, c_varint varint,
			  c_list list<int>, c_set set<text>, c_map map<text, int>,
			  c_frozen_list frozen<list<int>>, c_nested list<frozen<map<text, int>>>,
			  c_tuple tuple<int, text>, c_udt frozen<address>,
			  c_udt_map map<text, frozen<address>>, c_vector vector<float, 3>)""");
	}

	@AfterEach
	void tearDown() throws IOException {
		client.close();
		server.close();
		session.close();
	}

	@Test
	@DisplayName("every supported type round-trips over the wire, type and value both")
	void testEveryType() throws IOException, UnknownHostException {
		send("""
			INSERT INTO ks.t (id, c_ascii, c_bigint, c_blob, c_boolean, c_date, c_decimal, c_double,
			  c_duration, c_float, c_inet, c_int, c_smallint, c_text, c_time, c_timestamp,
			  c_timeuuid, c_tinyint, c_uuid, c_varint, c_list, c_set, c_map, c_frozen_list,
			  c_nested, c_tuple, c_udt, c_udt_map, c_vector)
			VALUES (1, 'ascii', 9223372036854775807, 0x0102, true, '2026-07-29', 1.25,
			  2.5, 3mo2d, 1.5, '127.0.0.1', 42, 7, 'text', '01:02:03.000000004',
			  '2026-07-29T00:00:00Z', %s, 3, %s, 123456789012345678901234567890,
			  [1, 2], {'a', 'b'}, {'k': 1}, [3, 4], [{'n': 5}], (6, 'seven'),
			  {street: 'Main', zip: 8}, {'home': {street: 'Elm', zip: 9}}, [1.5, 2.5, 3.5])"""
			.formatted(TIME_ID, ID));

		final var values = readRow();

		assertEquals("ascii", values.get("c_ascii"));
		assertEquals(Long.MAX_VALUE, values.get("c_bigint"));
		assertEquals(ByteBuffer.wrap(new byte[]{1, 2}), values.get("c_blob"));
		assertEquals(Boolean.TRUE, values.get("c_boolean"));
		assertEquals(LocalDate.of(2026, 7, 29), values.get("c_date"));
		assertEquals(new BigDecimal("1.25"), values.get("c_decimal"));
		assertEquals(2.5d, values.get("c_double"));
		assertEquals(CqlDuration.newInstance(3, 2, 0), values.get("c_duration"));
		assertEquals(1.5f, values.get("c_float"));
		assertEquals(InetAddress.getByName("127.0.0.1"), values.get("c_inet"));
		assertEquals(42, values.get("c_int"));
		assertEquals((short) 7, values.get("c_smallint"));
		assertEquals("text", values.get("c_text"));
		assertEquals(LocalTime.of(1, 2, 3, 4), values.get("c_time"));
		assertEquals(Instant.parse("2026-07-29T00:00:00Z"), values.get("c_timestamp"));
		assertEquals(TIME_ID, values.get("c_timeuuid"));
		assertEquals((byte) 3, values.get("c_tinyint"));
		assertEquals(ID, values.get("c_uuid"));
		assertEquals(new BigInteger("123456789012345678901234567890"), values.get("c_varint"));
		assertEquals(List.of(1, 2), values.get("c_list"));
		assertEquals(Set.of("a", "b"), values.get("c_set"));
		assertEquals(Map.of("k", 1), values.get("c_map"));
		assertEquals(List.of(3, 4), values.get("c_frozen_list"));
		assertEquals(List.of(Map.of("n", 5)), values.get("c_nested"));
		assertEquals(CqlVector.newInstance(1.5f, 2.5f, 3.5f), values.get("c_vector"));

		// A tuple and a UDT read back as driver values whose own fields have to be unpacked.
		final var tuple = (com.datastax.oss.driver.api.core.data.TupleValue) values.get("c_tuple");
		assertEquals(6, tuple.getInt(0));
		assertEquals("seven", tuple.getString(1));

		final var udt = (com.datastax.oss.driver.api.core.data.UdtValue) values.get("c_udt");
		assertEquals("Main", udt.getString("street"));
		assertEquals(8, udt.getInt("zip"));

		@SuppressWarnings("unchecked")
		final var udtMap = (Map<String, com.datastax.oss.driver.api.core.data.UdtValue>)
			values.get("c_udt_map");
		assertEquals("Elm", udtMap.get("home").getString("street"));
	}

	@Test
	@DisplayName("a column with no value comes back as a null buffer, whatever its type is")
	void testNulls() throws IOException {
		send("INSERT INTO ks.t (id) VALUES (1)");

		final var rows = assertInstanceOf(Rows.class, send("SELECT * FROM ks.t"));
		final var specs = rows.getMetadata().columnSpecs;
		final var row = rows.getData().peek();

		assertEquals(specs.size(), row.size());
		for (int i = 0; i < specs.size(); i++) {
			if ("id".equals(specs.get(i).name)) {
				continue;
			}
			assertNull(row.get(i), specs.get(i).name);
		}
	}

	@Test
	@DisplayName("the column type on the wire is the one the model holds, read the driver's way")
	void testTypesMatchTheModel() throws IOException {
		final var table = session.getMetadata()
			.getKeyspace(CqlIdentifier.fromInternal("ks"))
			.orElseThrow(() -> new IllegalStateException("the fixture keyspace should exist"))
			.getTable(CqlIdentifier.fromInternal("t"))
			.orElseThrow(() -> new IllegalStateException("the fixture table should exist"));

		final var rows = assertInstanceOf(Rows.class, send("SELECT * FROM ks.t"));
		for (final var spec : rows.getMetadata().columnSpecs) {
			final var column = table.getColumn(CqlIdentifier.fromInternal(spec.name))
				.orElseThrow(() -> new IllegalStateException(spec.name + " should be a column"));

			assertEquals(column.getType(), decodeType(spec), spec.name);
		}
	}

	/**
	 * Decodes the first row of the fixture table, keyed by column name, through the same path a
	 * connecting driver takes: the protocol type is turned back into a {@code DataType} and the
	 * codec registry supplies the codec for it.
	 */
	private Map<String, Object> readRow() throws IOException {
		final var rows = assertInstanceOf(Rows.class, send("SELECT * FROM ks.t"));
		final var specs = rows.getMetadata().columnSpecs;
		final var row = rows.getData().peek();
		final var values = new LinkedHashMap<String, Object>();
		for (int i = 0; i < specs.size(); i++) {
			final var type = decodeType(specs.get(i));
			values.put(specs.get(i).name,
				CodecRegistry.DEFAULT.codecFor(type).decode(row.get(i), ProtocolVersion.V4));
		}

		return values;
	}

	private static DataType decodeType(final ColumnSpec spec) {
		return DataTypeHelper.fromProtocolSpec(spec.type, AttachmentPoint.NONE);
	}

	private com.datastax.oss.protocol.internal.Message send(final String cql) throws IOException {
		return client.send(V4, streamIds.incrementAndGet() & 0x7FFF, new Query(cql)).message;
	}

}
