package com.tagadvance.seastar.server;

import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.driver.internal.core.protocol.BytesToSegmentDecoder;
import com.datastax.oss.driver.internal.core.protocol.FrameDecoder;
import com.datastax.oss.driver.internal.core.protocol.FrameEncoder;
import com.datastax.oss.driver.internal.core.protocol.FrameToSegmentEncoder;
import com.datastax.oss.driver.internal.core.protocol.SegmentToBytesEncoder;
import com.datastax.oss.driver.internal.core.protocol.SegmentToFrameDecoder;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.SegmentCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelPipeline;
import net.jcip.annotations.ThreadSafe;

/**
 * The two shapes a connection's pipeline takes, and the one-way switch between them.
 *
 * <p>Legacy framing is a length-prefixed frame per message and is what every connection starts in.
 * From protocol v5 on, everything after {@code READY} is instead wrapped in self-contained segments
 * carrying a CRC24 over the header and a CRC32 over the payload - several small frames may share
 * one segment, and one large frame may be split across several.
 *
 * <pre>
 * legacy   bytes &lt;-&gt; frames
 * modern   bytes &lt;-&gt; segments &lt;-&gt; frames
 * </pre>
 *
 * <p><strong>Not one line of framing is written here.</strong> All six handlers are the driver's
 * own, from {@code com.datastax.oss.driver.internal.core.protocol}, and every one of them is
 * direction-agnostic: they delegate to a {@link FrameCodec} or a {@link SegmentCodec} and the
 * direction lives in the codec ({@code defaultServer} rather than {@code defaultClient}), not in
 * the handler. That is the same finding b_plan B1 recorded for the legacy pair, checked again for
 * the segment four. The cost of the reuse is a compile-time dependency on driver internals, which
 * a driver bump breaks loudly at {@code compileJava}; {@code FrameRoundTripTest} says so where a
 * reader will meet it.
 *
 * <p>One instance per server: {@link FrameCodec}, {@link SegmentCodec} and
 * {@link ByteBufPrimitiveCodec} are stateless, and {@link FrameEncoder} and
 * {@link SegmentToBytesEncoder} are {@code @Sharable}. The decoders hold reassembly state and are
 * built per channel.
 */
@ThreadSafe
final class Framing {

	static final String FRAME_ENCODER = "frameEncoder";
	static final String SEGMENT_ENCODER = "segmentEncoder";
	static final String BYTES_ENCODER = "bytesEncoder";
	static final String FRAME_DECODER = "frameDecoder";
	static final String SEGMENT_DECODER = "segmentDecoder";
	static final String FRAME_FROM_SEGMENT_DECODER = "frameFromSegmentDecoder";

	private final int maxFrameLength;
	private final ByteBufPrimitiveCodec primitiveCodec;
	private final FrameCodec<ByteBuf> frameCodec;
	private final SegmentCodec<ByteBuf> segmentCodec;
	private final FrameEncoder frameEncoder;
	private final SegmentToBytesEncoder bytesEncoder;

	Framing(final int maxFrameLength) {
		this.maxFrameLength = maxFrameLength;
		this.primitiveCodec = new ByteBufPrimitiveCodec(ByteBufAllocator.DEFAULT);
		// No compression, so the segment header is three bytes rather than five and a payload
		// travels as it was written. b_plan B7 refuses compression by name; f_plan F4 leaves it
		// refused, since it buys nothing on a loopback socket.
		this.frameCodec = FrameCodec.defaultServer(primitiveCodec, Compressor.none());
		this.segmentCodec = new SegmentCodec<>(primitiveCodec, Compressor.none());
		this.frameEncoder = new FrameEncoder(frameCodec, maxFrameLength);
		this.bytesEncoder = new SegmentToBytesEncoder(segmentCodec);
	}

	/**
	 * @return the shared outbound encoder every connection starts with
	 */
	FrameEncoder frameEncoder() {
		return frameEncoder;
	}

	/**
	 * @return a fresh inbound decoder, which holds the reassembly state of one connection
	 */
	FrameDecoder frameDecoder() {
		return new FrameDecoder(frameCodec, maxFrameLength);
	}

	/**
	 * Rearranges a pipeline for protocol v5's segment framing, mid-stream, in place.
	 *
	 * <p>One conversion step is added in the middle in each direction. The order is the driver's
	 * own from {@code ProtocolInitHandler#maybeSwitchToModernFraming}, mirrored: outbound travels
	 * from the tail toward the head, so frames meet the segment encoder before the segment encoder's
	 * output meets the byte encoder.
	 *
	 * <p><strong>Call this on the channel's event loop, and only once the {@code READY} has already
	 * been encoded.</strong> v5 changes the framing of everything <em>after</em> that message, not
	 * of the message itself.
	 *
	 * @param pipeline the pipeline to rearrange
	 */
	void segmented(final ChannelPipeline pipeline) {
		final var logPrefix = String.valueOf(pipeline.channel());

		pipeline.replace(FRAME_ENCODER, SEGMENT_ENCODER,
			new FrameToSegmentEncoder(primitiveCodec, frameCodec, logPrefix));
		pipeline.addBefore(SEGMENT_ENCODER, BYTES_ENCODER, bytesEncoder);

		pipeline.replace(FRAME_DECODER, SEGMENT_DECODER, new BytesToSegmentDecoder(segmentCodec));
		pipeline.addAfter(SEGMENT_DECODER, FRAME_FROM_SEGMENT_DECODER,
			new SegmentToFrameDecoder(frameCodec, logPrefix));
	}
}
