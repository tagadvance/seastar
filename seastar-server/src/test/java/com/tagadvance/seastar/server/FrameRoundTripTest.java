package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.driver.internal.core.protocol.FrameDecoder;
import com.datastax.oss.driver.internal.core.protocol.FrameEncoder;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The tripwire for a driver bump.
 *
 * <p>{@code :seastar-server} writes no framing code of its own. It puts
 * {@link com.datastax.oss.driver.internal.core.protocol.FrameEncoder},
 * {@link com.datastax.oss.driver.internal.core.protocol.FrameDecoder} and
 * {@link com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec} - three classes the
 * <em>client</em> driver uses to talk to a node - into a <em>server</em> pipeline, and relies on all
 * three being direction-agnostic. They are, in 4.19.3: the direction lives entirely in the
 * {@link FrameCodec} they are handed ({@code defaultServer} against {@code defaultClient}), not in
 * the handlers.
 *
 * <p>That is a property of a package the driver marks internal, and it can stop being true without
 * a deprecation cycle. A version that <em>moves</em> one of these classes fails at
 * {@code compileJava} and needs no test. A version that keeps them and makes them client-only fails
 * here, and nowhere else - which is why every assertion below says so in its message rather than
 * merely going red.
 *
 * <p>Each test drives the handlers through an {@link EmbeddedChannel}, the same way the listener's
 * pipeline does, and checks the far side with the driver's own client codec. A message that goes out
 * of one and comes back out of the other unchanged is the whole claim.
 */
class FrameRoundTripTest {

	private static final int V4 = ProtocolConstants.Version.V4;

	/** The listener's own ceiling. Nothing in this test approaches it. */
	private static final int MAX_FRAME_LENGTH = 64 * 1024 * 1024;

	private static final String ENCODER_HAZARD =
		"com.datastax.oss.driver.internal.core.protocol.FrameEncoder no longer encodes a response "
			+ "the driver's own client codec can read. That package is internal to java-driver-core "
			+ "and seastar-server uses it in the server direction; a driver bump that makes it "
			+ "client-only breaks the listener and is caught nowhere else.";

	private static final String DECODER_HAZARD =
		"com.datastax.oss.driver.internal.core.protocol.FrameDecoder no longer decodes a request the "
			+ "driver's own client codec produced. That package is internal to java-driver-core and "
			+ "seastar-server uses it in the server direction; a driver bump that makes it "
			+ "client-only breaks the listener and is caught nowhere else.";

	private final FrameCodec<ByteBuf> serverCodec = FrameCodec.defaultServer(
		new ByteBufPrimitiveCodec(UnpooledByteBufAllocator.DEFAULT), Compressor.none());

	private final FrameCodec<ByteBuf> clientCodec = FrameCodec.defaultClient(
		new ByteBufPrimitiveCodec(UnpooledByteBufAllocator.DEFAULT), Compressor.none());

	@Test
	@DisplayName("a response the server encoder writes is read back by the driver's client codec")
	void testResponseRoundTrip() {
		final var ready = encodeResponse(7, new Ready());
		assertEquals(V4, ready.protocolVersion, ENCODER_HAZARD);
		assertEquals(7, ready.streamId, ENCODER_HAZARD);
		assertInstanceOf(Ready.class, ready.message, ENCODER_HAZARD);

		final var keyspace = assertInstanceOf(SetKeyspace.class,
			encodeResponse(8, new SetKeyspace("ks")).message, ENCODER_HAZARD);
		assertEquals("ks", keyspace.keyspace, ENCODER_HAZARD);

		final var error = assertInstanceOf(Error.class, encodeResponse(9,
				new Error(ProtocolConstants.ErrorCode.INVALID, "table t does not exist")).message,
			ENCODER_HAZARD);
		assertEquals(ProtocolConstants.ErrorCode.INVALID, error.code, ENCODER_HAZARD);
		assertEquals("table t does not exist", error.message, ENCODER_HAZARD);
	}

	@Test
	@DisplayName("a ROWS response survives the round trip with its metadata and every value")
	void testRowsRoundTrip() {
		final var specs = List.of(new ColumnSpec("ks", "t", "id", 0, RawType.PRIMITIVES.get(
				ProtocolConstants.DataType.INT)),
			new ColumnSpec("ks", "t", "name", 1, RawType.PRIMITIVES.get(
				ProtocolConstants.DataType.VARCHAR)));
		final var data = new ArrayDeque<List<ByteBuffer>>();
		data.add(List.of(ByteBuffer.allocate(4).putInt(0, 1), text("one")));
		// A null value is a length of -1 on the wire rather than an absent column, and it is the case
		// a round trip is most likely to lose.
		data.add(Arrays.asList(ByteBuffer.allocate(4).putInt(0, 2), null));

		final var decoded = encodeResponse(1,
			new DefaultRows(new RowsMetadata(specs, null, null, null), data));

		final var rows = assertInstanceOf(Rows.class, decoded.message, ENCODER_HAZARD);
		assertEquals(List.of("id", "name"),
			rows.getMetadata().columnSpecs.stream().map(spec -> spec.name).toList(),
			ENCODER_HAZARD);
		assertNull(rows.getMetadata().pagingState, ENCODER_HAZARD);

		final var first = rows.getData().poll();
		assertNotNull(first, ENCODER_HAZARD);
		assertEquals(1, first.get(0).getInt(), ENCODER_HAZARD);
		assertEquals("one", StandardCharsets.UTF_8.decode(first.get(1)).toString(), ENCODER_HAZARD);

		final var second = rows.getData().poll();
		assertNotNull(second, ENCODER_HAZARD);
		assertEquals(2, second.get(0).getInt(), ENCODER_HAZARD);
		assertNull(second.get(1), ENCODER_HAZARD);
	}

	@Test
	@DisplayName("a request the driver's client codec writes is read back by the server decoder")
	void testRequestRoundTrip() {
		final var options = new QueryOptions(ProtocolConstants.ConsistencyLevel.ONE,
			List.of(ByteBuffer.allocate(4).putInt(0, 42)), Map.of(), false, 5000, null,
			ProtocolConstants.ConsistencyLevel.SERIAL, QueryOptions.NO_DEFAULT_TIMESTAMP, null,
			QueryOptions.NO_NOW_IN_SECONDS);
		final var request = new Query("SELECT * FROM ks.t WHERE id = ?", options);

		final var decoded = decodeRequest(11, request, 1);

		assertEquals(V4, decoded.protocolVersion, DECODER_HAZARD);
		assertEquals(11, decoded.streamId, DECODER_HAZARD);
		final var query = assertInstanceOf(Query.class, decoded.message, DECODER_HAZARD);
		assertEquals(request.query, query.query, DECODER_HAZARD);
		assertEquals(5000, query.options.pageSize, DECODER_HAZARD);
		assertEquals(42, query.options.positionalValues.get(0).getInt(), DECODER_HAZARD);

		// STARTUP is the first frame of every connection, so it is the one whose loss is total.
		assertInstanceOf(Startup.class, decodeRequest(0, new Startup(), 1).message, DECODER_HAZARD);
	}

	@Test
	@DisplayName("a request split across four reads is reassembled rather than lost")
	void testRequestSplitAcrossReads() {
		// The reassembly is the half of FrameDecoder that is not FrameCodec: it is a
		// LengthFieldBasedFrameDecoder, and a socket is free to deliver a frame in as many pieces as
		// it likes. Nothing else in the suite makes it do so, because a loopback socket rarely will.
		final var decoded = decodeRequest(3,
			new Query("SELECT * FROM ks.t", QueryOptions.DEFAULT), 4);

		final var query = assertInstanceOf(Query.class, decoded.message, DECODER_HAZARD);
		assertEquals("SELECT * FROM ks.t", query.query, DECODER_HAZARD);
	}

	/**
	 * Encodes a response through the listener's own encoder and reads it back with the driver's
	 * client codec, which is what a connected driver does with it.
	 *
	 * @param streamId the stream id to answer on
	 * @param message  the response to send
	 * @return the frame the client codec made of it
	 */
	private Frame encodeResponse(final int streamId, final Message message) {
		final var channel = new EmbeddedChannel(new FrameEncoder(serverCodec, MAX_FRAME_LENGTH));
		try {
			assertTrue(channel.writeOutbound(
					Frame.forResponse(V4, streamId, null, Frame.NO_PAYLOAD, List.of(), message)),
				ENCODER_HAZARD);
			final ByteBuf encoded = channel.readOutbound();
			assertNotNull(encoded, ENCODER_HAZARD);

			try {
				return clientCodec.decode(encoded);
			} finally {
				encoded.release();
			}
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	/**
	 * Encodes a request with the driver's client codec and reads it back through the listener's own
	 * decoder, which is what the listener does with it.
	 *
	 * @param streamId the stream id to send on
	 * @param message  the request to send
	 * @param chunks   how many reads to split the bytes across
	 * @return the frame the decoder made of it
	 */
	private Frame decodeRequest(final int streamId, final Message message, final int chunks) {
		final var encoded = clientCodec.encode(
			Frame.forRequest(V4, streamId, false, Frame.NO_PAYLOAD, message));
		final byte[] bytes;
		try {
			bytes = new byte[encoded.readableBytes()];
			encoded.readBytes(bytes);
		} finally {
			encoded.release();
		}

		final var channel = new EmbeddedChannel(new FrameDecoder(serverCodec, MAX_FRAME_LENGTH));
		try {
			final var size = (bytes.length + chunks - 1) / chunks;
			for (int offset = 0; offset < bytes.length; offset += size) {
				final var length = Math.min(size, bytes.length - offset);
				channel.writeInbound(Unpooled.copiedBuffer(bytes, offset, length));
			}

			final Frame decoded = channel.readInbound();
			assertNotNull(decoded,
				"the decoder produced no frame from " + bytes.length + " bytes. " + DECODER_HAZARD);

			return decoded;
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	private static ByteBuffer text(final String value) {
		return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

}
