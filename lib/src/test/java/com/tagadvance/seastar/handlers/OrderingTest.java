package com.tagadvance.seastar.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.tagadvance.seastar.SeaStarCqlSession;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarTable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The read path's ordering, checked without a query running: the order each type sorts in, the
 * Murmur3 token a partition key hashes to, and how a table's rows come out of the two together.
 *
 * <p>Every expectation here was read off a live {@code cassandra:5.0.8} node rather than out of the
 * documentation, because the cases worth covering are the ones where Cassandra's order is not the
 * obvious Java one.
 */
class OrderingTest {

	private SeaStarCqlSession session;
	private SeaStarDriverContext context;

	@BeforeEach
	void beforeEach() {
		session = SeaStarCqlSession.builder().build();
		context = session.getContext();
	}

	@AfterEach
	void afterEach() {
		session.close();
	}

	static Stream<Arguments> types() throws UnknownHostException {
		return Stream.of(
			arguments(DataTypes.INT, List.of(Integer.MIN_VALUE, -5, -1, 0, 1, 5, Integer.MAX_VALUE)),
			arguments(DataTypes.BIGINT, List.of(Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE)),
			arguments(DataTypes.SMALLINT,
				List.of(Short.MIN_VALUE, (short) -1, (short) 0, Short.MAX_VALUE)),
			arguments(DataTypes.TINYINT, List.of(Byte.MIN_VALUE, (byte) -1, (byte) 0, Byte.MAX_VALUE)),
			arguments(DataTypes.VARINT, List.of(new BigInteger("-99999999999999999999"),
				BigInteger.ZERO, new BigInteger("99999999999999999999"))),
			arguments(DataTypes.DECIMAL, List.of(new BigDecimal("-1.5"), new BigDecimal("0"),
				new BigDecimal("1.25"), new BigDecimal("1.3"))),
			arguments(DataTypes.FLOAT, List.of(-1.5f, 0f, 1.5f, Float.MAX_VALUE)),
			arguments(DataTypes.DOUBLE, List.of(-1.5d, 0d, 1.5d, Double.MAX_VALUE)),
			arguments(DataTypes.BOOLEAN, List.of(false, true)),
			// UTF-8 byte order, which is code point order: digits, then capitals, then lower case,
			// then everything outside ASCII.
			// Escaped rather than literal, so the build does not depend on its source encoding.
			arguments(DataTypes.TEXT,
				List.of("", "1", "A", "B", "a", "b", "\u00e9", "\ud83d\ude00")),
			arguments(DataTypes.ASCII, List.of("", "1", "A", "a")),
			// Unsigned bytes, and a prefix sorts before what extends it.
			arguments(DataTypes.BLOB, List.of(buffer(0x00), buffer(0x01), buffer(0x01, 0x00),
				buffer(0x7f), buffer(0xff))),
			// The raw address, so an IPv6 address beginning with zeroes leads and an IPv4 one
			// compares as its four unsigned bytes.
			arguments(DataTypes.INET, List.of(InetAddress.getByName("::1"),
				InetAddress.getByName("1.2.3.4"), InetAddress.getByName("129.0.0.1"),
				InetAddress.getByName("255.255.255.255"))),
			arguments(DataTypes.DATE, List.of(LocalDate.of(1969, 12, 31), LocalDate.of(1970, 1, 1),
				LocalDate.of(2026, 7, 26))),
			arguments(DataTypes.TIME, List.of(LocalTime.MIDNIGHT, LocalTime.NOON, LocalTime.MAX)),
			arguments(DataTypes.TIMESTAMP,
				List.of(Instant.EPOCH.minusSeconds(1), Instant.EPOCH, Instant.EPOCH.plusSeconds(1))),
			// A uuid orders on its version first, then unsigned on each half - so a version 1 UUID
			// sorts before every version 4 one however large its bits are.
			arguments(DataTypes.UUID, List.of(UUID.fromString("00000000-0000-1000-8000-000000000000"),
				UUID.fromString("7fffffff-0000-1000-8000-000000000000"),
				UUID.fromString("80000000-0000-1000-8000-000000000000"),
				UUID.fromString("ffffffff-0000-1000-8000-000000000000"),
				UUID.fromString("00000000-0000-4000-8000-000000000000"))),
			// A timeuuid orders on the timestamp its bits are scattered across, not on the bits.
			arguments(DataTypes.TIMEUUID,
				List.of(UUID.fromString("ffffffff-ffff-1000-8000-000000000000"),
					UUID.fromString("00000000-0000-1001-8000-000000000000"),
					UUID.fromString("00000000-0000-1002-8000-000000000000"))));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("types")
	@DisplayName("Every type sorts the way Cassandra sorts it, not the way Comparable does")
	void comparators(final DataType type, final List<Object> ascending) {
		final var comparator = ValueComparators.of(type, context.getCodecRegistry(),
			context.getProtocolVersion());

		final var shuffled = new ArrayList<>(ascending);
		Collections.reverse(shuffled);
		shuffled.sort(comparator);

		assertEquals(ascending, shuffled);
	}

	@Test
	@DisplayName("A null sorts before every value rather than failing the comparison")
	void nulls() {
		final var comparator = ValueComparators.of(DataTypes.INT, context.getCodecRegistry(),
			context.getProtocolVersion());

		final var values = new ArrayList<Object>(Arrays.asList(2, 1, null));
		values.sort(comparator);

		assertEquals(Arrays.asList(null, 1, 2), values);
	}

	@Test
	@DisplayName("A type with no comparator of its own falls back to unsigned byte order")
	void fallback() {
		final var comparator = ValueComparators.of(DataTypes.frozenListOf(DataTypes.INT),
			context.getCodecRegistry(), context.getProtocolVersion());

		final var values = new ArrayList<Object>(List.of(List.of(1, 1), List.of(2), List.of(1)));
		values.sort(comparator);

		// A frozen list is encoded as its element count and then its elements, so one element
		// sorts before two and the elements decide within a count.
		assertEquals(List.of(List.of(1), List.of(2), List.of(1, 1)), values);
	}

	@Test
	@DisplayName("A single-column partition key hashes to the token a real node computes")
	void tokensOfSingleColumnKeys() {
		// Read off cassandra:5.0.8 with SELECT token(pk) FROM t.
		final var expected = List.of(-4069959284402364209L, -3248873570005575792L,
			9010454139840013625L, -2729420104000364805L, -7509452495886106294L);

		final var tokens = Stream.of(1, 2, 3, 4, 5)
			.map(value -> TypeCodecs.INT.encode(value, context.getProtocolVersion()))
			.map(key -> Tokens.of(Tokens.encode(List.of(key))))
			.toList();

		assertEquals(expected, tokens);
	}

	@Test
	@DisplayName("A composite partition key hashes the way CompositeType encodes it")
	void tokensOfCompositeKeys() {
		// Read off cassandra:5.0.8 with SELECT token(a, b) FROM t for keys (1, 'x1') .. (5, 'x5').
		final var expected = List.of(2200253482409776311L, 6436619808079780028L,
			8154742290845834743L, -2084475353302800008L, -1014509722901203742L);

		final var version = context.getProtocolVersion();
		final var tokens = Stream.of(1, 2, 3, 4, 5)
			.map(value -> Tokens.encode(List.of(TypeCodecs.INT.encode(value, version),
				TypeCodecs.TEXT.encode("x" + value, version))))
			.map(Tokens::of)
			.toList();

		assertEquals(expected, tokens);
	}

	@Test
	@DisplayName("Rows come back in token order, then in clustering order within a partition")
	void ordersPartitionsThenRows() {
		final var table = table(ClusteringOrder.ASC);
		// Neither the partitions nor the rows in them are added in the order they read back in.
		Stream.of(4, 3, 2, 1).forEach(pk -> Stream.of(2, 1, 3).forEach(ck -> table.addRow(pk, ck)));

		// Partition keys 1..4 hash to tokens that put partition 4 ahead of partition 3.
		assertEquals(List.of("1/1", "1/2", "1/3", "2/1", "2/2", "2/3", "4/1", "4/2", "4/3", "3/1",
			"3/2", "3/3"), sorted(table, false));
	}

	@Test
	@DisplayName("A clustering column declared DESC reads back descending")
	void ordersDescendingClustering() {
		final var table = table(ClusteringOrder.DESC);
		Stream.of(2, 1, 3).forEach(ck -> table.addRow(1, ck));

		assertEquals(List.of("1/3", "1/2", "1/1"), sorted(table, false));
	}

	@Test
	@DisplayName("A reversed read walks the clustering order backwards, leaving partitions alone")
	void reversesClusteringOnly() {
		final var table = table(ClusteringOrder.ASC);
		Stream.of(1, 2).forEach(pk -> Stream.of(2, 1).forEach(ck -> table.addRow(pk, ck)));

		assertEquals(List.of("1/1", "1/2", "2/1", "2/2"), sorted(table, false));
		assertEquals(List.of("1/2", "1/1", "2/2", "2/1"), sorted(table, true));
	}

	private SeaStarTable table(final ClusteringOrder order) {
		final var table = context.newSeaStarKeyspace("ks").newSeaStarTable("t");
		table.addColumn("pk", DataTypes.INT);
		table.addColumn("ck", DataTypes.INT);
		table.markPartitionKey(CqlIdentifier.fromInternal("pk"));
		table.markClustering(CqlIdentifier.fromInternal("ck"), order);

		return table;
	}

	/**
	 * The ordered rows as {@code pk/ck}, so an assertion reads as the partitions and the rows
	 * inside them rather than as a list of objects.
	 */
	private static List<String> sorted(final SeaStarTable table, final boolean reversed) {
		return RowOrdering.of(table, reversed).sort(table.rows()).map(OrderingTest::label).toList();
	}

	private static String label(final SeaStarRow row) {
		return row.getObject(0) + "/" + row.getObject(1);
	}

	private static ByteBuffer buffer(final int... bytes) {
		final var buffer = ByteBuffer.allocate(bytes.length);
		for (final var value : bytes) {
			buffer.put((byte) value);
		}

		return buffer.flip();
	}

}
