package com.tagadvance.seastar.handlers;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.utils.MurmurHash;

/**
 * Murmur3 partition tokens, which is the order Cassandra returns partitions in.
 *
 * <p>This is the real thing, not a stand-in: {@code Murmur3Partitioner} computes exactly
 * {@code MurmurHash.hash3_x64_128(key)[0]}, with {@code Long.MIN_VALUE} reserved as the minimum
 * token and folded onto {@code Long.MAX_VALUE}. The partitioner itself is deliberately not used -
 * loading it drags in {@code PartitionerDefinedOrder} and the {@code AbstractType} hierarchy, and
 * the reason c_plan C1 rejects {@code Raw#prepare} is that touching that side of cassandra-all
 * costs a {@code DatabaseDescriptor} init. {@code MurmurHash} is a leaf utility with no static
 * state, so calling it costs nothing but the hash.
 *
 * <p>A composite partition key is hashed the way {@code CompositeType} encodes it: each component
 * as an unsigned 2-byte length, its bytes, then a zero end-of-component byte. Verified against a
 * live node - the tokens match, component for component.
 */
final class Tokens {

	private Tokens() {
		// hidden constructor
	}

	/**
	 * The token of an already-encoded partition key.
	 */
	static long of(final ByteBuffer key) {
		if (!key.hasRemaining()) {
			return Long.MIN_VALUE;
		}

		final var hash = new long[2];
		MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0, hash);

		return hash[0] == Long.MIN_VALUE ? Long.MAX_VALUE : hash[0];
	}

	/**
	 * The encoded form of a partition key, which is the single component's bytes when there is only
	 * one and the composite encoding when there are several.
	 *
	 * @param components the encoded value of each partition key column, in key order
	 */
	static ByteBuffer encode(final List<ByteBuffer> components) {
		if (components.size() == 1) {
			final var only = components.get(0);

			return only == null ? ByteBuffer.allocate(0) : only.duplicate();
		}

		var size = 0;
		for (final var component : components) {
			size += 3 + (component == null ? 0 : component.remaining());
		}
		final var key = ByteBuffer.allocate(size);
		for (final var component : components) {
			final var length = component == null ? 0 : component.remaining();
			key.putShort((short) length);
			if (component != null) {
				key.put(component.duplicate());
			}
			key.put((byte) 0);
		}

		return key.flip();
	}

}
