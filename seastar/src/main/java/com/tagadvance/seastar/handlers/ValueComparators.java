package com.tagadvance.seastar.handlers;

import static java.util.Map.entry;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

/**
 * The order Cassandra puts values of a given type in.
 *
 * <p>Sorting a clustering column by {@link Comparable} is wrong often enough to matter, so every
 * order here is the one Cassandra's own {@code AbstractType} defines. Three of them disagree with
 * the obvious Java answer:
 *
 * <ul>
 *   <li>{@code text} is compared by code point, because Cassandra compares the UTF-8 bytes and
 *       {@link String#compareTo} compares UTF-16 code units - the two differ above U+FFFF.
 *   <li>{@code uuid} compares the version first, then time-orders a version 1 UUID and unsigned-
 *       compares any other, where {@link UUID#compareTo} compares both halves as signed longs.
 *   <li>{@code timeuuid} time-orders every value, and compares the low half byte by signed byte.
 * </ul>
 *
 * <p>Anything with no entry in the table - a frozen collection, a tuple, a UDT, a custom type -
 * falls back to comparing the encoded bytes unsigned. That is exactly right for the types Cassandra
 * declares {@code BYTE_ORDER} and an approximation for the rest; it is deterministic either way,
 * which is the property the read path depends on.
 */
final class ValueComparators {

	private static final Comparator<ByteBuffer> UNSIGNED = ValueComparators::compareUnsigned;

	private static final Comparator<Object> TEXT = comparator(String.class,
		ValueComparators::compareCodePoints);

	private static final Map<DataType, Comparator<Object>> BY_TYPE = Map.ofEntries(
		entry(DataTypes.ASCII, TEXT),
		entry(DataTypes.TEXT, TEXT),
		entry(DataTypes.TINYINT, natural(Byte.class)),
		entry(DataTypes.SMALLINT, natural(Short.class)),
		entry(DataTypes.INT, natural(Integer.class)),
		entry(DataTypes.BIGINT, natural(Long.class)),
		entry(DataTypes.COUNTER, natural(Long.class)),
		entry(DataTypes.VARINT, natural(BigInteger.class)),
		entry(DataTypes.DECIMAL, natural(BigDecimal.class)),
		entry(DataTypes.FLOAT, natural(Float.class)),
		entry(DataTypes.DOUBLE, natural(Double.class)),
		entry(DataTypes.BOOLEAN, natural(Boolean.class)),
		entry(DataTypes.BLOB, comparator(ByteBuffer.class, UNSIGNED)),
		entry(DataTypes.INET, comparator(InetAddress.class, ValueComparators::compareAddresses)),
		entry(DataTypes.DATE, natural(LocalDate.class)),
		entry(DataTypes.TIME, natural(LocalTime.class)),
		entry(DataTypes.TIMESTAMP, natural(Instant.class)),
		entry(DataTypes.UUID, comparator(UUID.class, ValueComparators::compareUuids)),
		entry(DataTypes.TIMEUUID, comparator(UUID.class, ValueComparators::compareTimeUuids)));

	private ValueComparators() {
		// hidden constructor
	}

	/**
	 * The comparator for values of {@code type}, as the driver decodes them. Nulls sort first: no
	 * primary key part may be null in Cassandra, but a comparator that threw on one would turn a
	 * malformed row into a failed query rather than an ordered one.
	 *
	 * @param codecRegistry the registry used to encode a type the table above does not cover
	 * @param version       the protocol version that encoding is done at
	 */
	static Comparator<Object> of(final DataType type, final CodecRegistry codecRegistry,
		final ProtocolVersion version) {
		final var comparator = BY_TYPE.get(type);
		if (comparator != null) {
			return Comparator.nullsFirst(comparator);
		}

		final TypeCodec<Object> codec = codecRegistry.codecFor(type);

		return Comparator.nullsFirst(Comparator.comparing(value -> codec.encode(value, version),
			Comparator.nullsFirst(UNSIGNED)));
	}

	/**
	 * Unsigned lexicographic order over raw bytes, which is how Cassandra orders a partition key of
	 * equal token and every type it declares {@code BYTE_ORDER}.
	 */
	static Comparator<ByteBuffer> unsignedBytes() {
		return UNSIGNED;
	}

	private static <T extends Comparable<? super T>> Comparator<Object> natural(
		final Class<T> type) {
		return comparator(type, Comparable::compareTo);
	}

	/**
	 * Adapts a typed comparator to the untyped values a row hands back. The cast asserts that a
	 * stored value matches its column's type, which {@code SeaStarRow#validate} enforces on the way
	 * in.
	 */
	private static <T> Comparator<Object> comparator(final Class<T> type,
		final Comparator<T> comparator) {
		return Comparator.comparing((Function<Object, T>) type::cast, comparator);
	}

	/**
	 * Cassandra compares {@code text} as UTF-8 bytes, whose order is code point order.
	 */
	private static int compareCodePoints(final String left, final String right) {
		int i = 0;
		int j = 0;
		while (i < left.length() && j < right.length()) {
			final var a = left.codePointAt(i);
			final var b = right.codePointAt(j);
			if (a != b) {
				return Integer.compare(a, b);
			}
			i += Character.charCount(a);
			j += Character.charCount(b);
		}

		return Integer.compare(left.length() - i, right.length() - j);
	}

	private static int compareUnsigned(final ByteBuffer left, final ByteBuffer right) {
		final var shared = Math.min(left.remaining(), right.remaining());
		for (int i = 0; i < shared; i++) {
			final var difference = Byte.toUnsignedInt(left.get(left.position() + i))
				- Byte.toUnsignedInt(right.get(right.position() + i));
			if (difference != 0) {
				return difference;
			}
		}

		return Integer.compare(left.remaining(), right.remaining());
	}

	private static int compareUnsigned(final byte[] left, final byte[] right) {
		final var shared = Math.min(left.length, right.length);
		for (int i = 0; i < shared; i++) {
			final var difference = Byte.toUnsignedInt(left[i]) - Byte.toUnsignedInt(right[i]);
			if (difference != 0) {
				return difference;
			}
		}

		return Integer.compare(left.length, right.length);
	}

	/**
	 * {@code inet} is byte ordered on the raw address, so an IPv4 address sorts as its four bytes
	 * and an IPv6 address as its sixteen.
	 */
	private static int compareAddresses(final InetAddress left, final InetAddress right) {
		return compareUnsigned(left.getAddress(), right.getAddress());
	}

	private static int compareUuids(final UUID left, final UUID right) {
		if (left.version() != right.version()) {
			return Integer.compare(left.version(), right.version());
		}

		final var high = left.version() == 1 ? Long.compare(
			reorderTimestampBytes(left.getMostSignificantBits()),
			reorderTimestampBytes(right.getMostSignificantBits()))
			: Long.compareUnsigned(left.getMostSignificantBits(), right.getMostSignificantBits());

		return high != 0 ? high
			: Long.compareUnsigned(left.getLeastSignificantBits(), right.getLeastSignificantBits());
	}

	private static int compareTimeUuids(final UUID left, final UUID right) {
		final var high = Long.compare(reorderTimestampBytes(left.getMostSignificantBits()),
			reorderTimestampBytes(right.getMostSignificantBits()));

		return high != 0 ? high
			: Long.compare(signedBytesToNativeLong(left.getLeastSignificantBits()),
				signedBytesToNativeLong(right.getLeastSignificantBits()));
	}

	/**
	 * A version 1 UUID scatters its timestamp across the high half as [mid-low][high]; putting the
	 * pieces back in order makes one signed comparison equivalent to comparing timestamps.
	 */
	private static long reorderTimestampBytes(final long input) {
		return (input << 48) | ((input << 16) & 0xFFFF00000000L) | (input >>> 32);
	}

	/**
	 * {@code timeuuid} compares the low half byte by <em>signed</em> byte, which one long
	 * comparison reproduces once the sign bits are flipped.
	 */
	private static long signedBytesToNativeLong(final long signedBytes) {
		return signedBytes ^ 0x0080808080808080L;
	}

}
