package com.tagadvance.seastar;

import static com.google.common.base.Preconditions.checkArgument;
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
			checkArgument(values.size() == size(), "Expected %d values but got %d", size(),
				values.size());

			final var codecRegistry = type.getAttachmentPoint().getCodecRegistry();
			for (int i = 0; i < values.size(); i++) {
				final var dataType = type.getFieldTypes().get(i);
				final var codec = codecRegistry.codecFor(dataType);
				checkArgument(codec.accepts(values.get(i)),
					"Value %d (%s) is not compatible with column type %s", i, values.get(i),
					dataType);
			}
		});
	}

}
