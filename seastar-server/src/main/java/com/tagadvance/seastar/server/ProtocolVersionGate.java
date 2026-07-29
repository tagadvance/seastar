package com.tagadvance.seastar.server;

import static java.util.Objects.requireNonNull;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import net.jcip.annotations.NotThreadSafe;

/**
 * Turns a connection away at the first header if it opens at a protocol version this server does
 * not speak, before the frame decoder ever sees it.
 *
 * <p>It has to happen ahead of decoding rather than after it, and that is not a stylistic
 * preference. A driver with no configured version does <strong>not</strong> start at v5: its
 * registry covers the DSE versions too, so {@code highestNonBeta()} is {@code DSE_V2}, whose
 * version byte is 66. {@code FrameCodec} has codecs for v3 to v6 only, so such a frame does not
 * decode at all and there is no stream to reject politely - the client would get a generic
 * decoding error, never see {@link Protocol#unsupportedVersion(int)}, and give up at its first
 * {@code OPTIONS}. That is precisely the "it just hangs" failure b_plan B4 exists to prevent.
 *
 * <p>The version is settled once per connection and never changes, so the gate removes itself as
 * soon as it has seen a header it likes. The driver's own {@code FrameDecoder} takes the same
 * shortcut with its {@code isFirstResponse} flag.
 */
@NotThreadSafe
final class ProtocolVersionGate extends ByteToMessageDecoder {

	private final SeaStarConnection connection;

	private boolean refused;

	ProtocolVersionGate(final SeaStarConnection connection) {
		this.connection = requireNonNull(connection, "connection must not be null");
	}

	@Override
	protected void decode(final ChannelHandlerContext ctx, final ByteBuf in,
		final List<Object> out) {
		if (refused) {
			// The refusal is already on its way out and the channel is closing behind it. Nothing
			// that arrives after it can be answered, because the stream is no longer aligned.
			in.skipBytes(in.readableBytes());

			return;
		}
		if (in.readableBytes() < Protocol.HEADER_LENGTH) {
			return;
		}

		final var version = in.getByte(in.readerIndex()) & 0b0111_1111;
		if (Protocol.speaks(version)) {
			// The version is settled for this connection now, and every event pushed to it later is
			// written with it.
			connection.version(version);
			// Deliberately consuming nothing: removing this handler makes ByteToMessageDecoder
			// forward everything it has accumulated to the decoder behind it, so the frame this
			// header belongs to arrives there intact.
			ctx.pipeline().remove(this);

			return;
		}

		refused = true;
		final int streamId = in.getShort(in.readerIndex() + 2);
		in.skipBytes(in.readableBytes());
		// Written at the highest version this server speaks rather than at the one that was asked
		// for, which is what cassandra:5.0.8 does and is the only choice that always decodes: the
		// version being refused may be one no frame codec can write.
		Protocol.write(ctx, Protocol.HIGHEST, streamId, Protocol.unsupportedVersion(version))
			.addListener(ChannelFutureListener.CLOSE);
	}
}
