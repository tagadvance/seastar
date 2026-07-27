package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.tagadvance.tools.SeaStarReadWriteLock;
import java.util.List;
import net.jcip.annotations.NotThreadSafe;
import org.jspecify.annotations.NonNull;

@NotThreadSafe
public interface SeaStarUdtValue extends SeaStarReadWriteLock, UdtValue {

	@NonNull SeaStarUserDefinedType getType();

	default void validate(final @NonNull List<Object> values)
		throws IllegalArgumentException {
		requireNonNull(values, "values must not be null");

		final var type = getType();
		type.readLock(() -> {
			// Measure against the field count, not size(): validate runs during construction
			// before the value slots are populated. newValue(Object...) may provide fewer values
			// than fields, filling only the leading slots.
			final var fieldCount = type.getFieldTypes().size();
			if (values.size() > fieldCount) {
				throw new IllegalArgumentException(
					"Expected at most %d values but got %d".formatted(fieldCount, values.size()));
			}

			final var codecRegistry = type.getAttachmentPoint().getCodecRegistry();
			for (int i = 0; i < values.size(); i++) {
				final var dataType = type.getFieldTypes().get(i);
				final var codec = codecRegistry.codecFor(dataType);
				if (!codec.accepts(values.get(i))) {
					throw new IllegalArgumentException(
						"Value %d (%s) is not compatible with column type %s".formatted(i,
							values.get(i), dataType));
				}
			}
		});
	}

}
