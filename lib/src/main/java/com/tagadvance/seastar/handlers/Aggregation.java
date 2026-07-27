package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.tagadvance.seastar.SeaStarRow;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import org.jspecify.annotations.Nullable;

/**
 * The running value of one aggregate selector over the rows a query matched.
 *
 * <p>A query that uses any aggregate returns exactly one row, and it returns it even when nothing
 * matched, which is why the empty answer is part of the fold rather than a special case around it:
 * {@code count} answers zero, {@code sum} and {@code avg} answer a typed zero, and {@code min} and
 * {@code max} answer null. That is what a cluster does.
 */
final class Aggregation {

	private final Selector selector;
	private final Comparator<Object> order;

	private long count;
	private @Nullable Object extreme;
	private @Nullable BigDecimal total;

	Aggregation(final Selector selector, final CodecRegistry codecRegistry,
		final ProtocolVersion version) {
		this.selector = selector;
		this.order = selector.aggregate() == Selector.Aggregate.MIN
			|| selector.aggregate() == Selector.Aggregate.MAX
			? ValueComparators.of(selector.type(), codecRegistry, version) : null;
	}

	void accumulate(final SeaStarRow row) {
		if (selector.aggregate() == Selector.Aggregate.COUNT_ROWS) {
			count++;

			return;
		}

		final var value = selector.reader().read(row);
		// Every aggregate but count(*) skips nulls, so a column that was never written neither counts
		// towards an average nor becomes a minimum.
		if (value == null) {
			return;
		}
		count++;
		switch (selector.aggregate()) {
			case MIN -> extreme = extreme == null || order.compare(value, extreme) < 0 ? value
				: extreme;
			case MAX -> extreme = extreme == null || order.compare(value, extreme) > 0 ? value
				: extreme;
			case SUM, AVG -> total = (total == null ? BigDecimal.ZERO : total).add(
				new BigDecimal(value.toString()));
			default -> {
				// count(c) needs nothing beyond the increment above
			}
		}
	}

	@Nullable
	Object result() {
		return switch (selector.aggregate()) {
			case COUNT_ROWS, COUNT -> count;
			case MIN, MAX -> extreme;
			case SUM -> narrow(total == null ? BigDecimal.ZERO : total);
			case AVG -> narrow(total == null || count == 0 ? BigDecimal.ZERO
				: total.divide(BigDecimal.valueOf(count), scale(selector.type()),
					RoundingMode.DOWN));
		};
	}

	/**
	 * The number of decimal places an average keeps, which is the column's own: an int column
	 * averages to an int, so {@code avg} of 10, 20 and 5 is 11 rather than 11.67.
	 */
	private static int scale(final DataType type) {
		if (DataTypes.FLOAT.equals(type) || DataTypes.DOUBLE.equals(type)) {
			return 17;
		}
		if (DataTypes.DECIMAL.equals(type)) {
			return 8;
		}

		return 0;
	}

	/**
	 * The running total, returned as the type the column holds - which is also the type the result
	 * column declares, so it has to be that type exactly rather than merely equal to it.
	 */
	private Object narrow(final BigDecimal value) {
		final var type = selector.type();
		if (DataTypes.TINYINT.equals(type)) {
			return value.byteValue();
		}
		if (DataTypes.SMALLINT.equals(type)) {
			return value.shortValue();
		}
		if (DataTypes.INT.equals(type)) {
			return value.intValue();
		}
		if (DataTypes.BIGINT.equals(type) || DataTypes.COUNTER.equals(type)) {
			return value.longValue();
		}
		if (DataTypes.FLOAT.equals(type)) {
			return value.floatValue();
		}
		if (DataTypes.DOUBLE.equals(type)) {
			return value.doubleValue();
		}
		if (DataTypes.VARINT.equals(type)) {
			return value.toBigInteger();
		}

		return value.stripTrailingZeros();
	}

}
