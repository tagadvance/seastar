package com.tagadvance.seastar.server;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import net.jcip.annotations.ThreadSafe;

/**
 * Where the wire meets the model: a decoded {@code QUERY}, {@code PREPARE}, {@code EXECUTE} or
 * {@code BATCH} arrives, and a response message has to come back.
 *
 * <p>Two things hold for every call, and the rest of the server exists to make them hold:
 *
 * <ul>
 *   <li><strong>It runs on the funnel</strong> - the server's single-threaded executor, never a
 *       Netty event loop - so no two requests are ever in the session at once, whichever
 *       connection they arrived on.</li>
 *   <li><strong>A thrown exception is caught by the caller</strong>
 *       ({@link SeaStarProtocolHandler}) and turned into a {@code SERVER_ERROR} carrying its
 *       message. Returning an {@link Error} is how a failure with a more specific code is
 *       reported; throwing is the fallback, not the channel.</li>
 * </ul>
 *
 * <p>Handshake messages do not come through here. {@code OPTIONS}, {@code STARTUP},
 * {@code REGISTER} and {@code AUTH_RESPONSE} are transport, and the handler answers them itself.
 */
@ThreadSafe
final class SeaStarRequestDispatcher {

	@SuppressWarnings("unused") // the seam: requests are not translated into session calls yet
	private final CqlSession session;

	SeaStarRequestDispatcher(final CqlSession session) {
		this.session = requireNonNull(session, "session must not be null");
	}

	/**
	 * @param request    the decoded request frame, always at protocol v4
	 * @param connection the state of the connection it arrived on
	 * @return the message to send back, on the request's own stream id
	 */
	Message dispatch(final Frame request, final SeaStarConnection connection) {
		return new Error(ProtocolConstants.ErrorCode.SERVER_ERROR,
			"SeaStar's listener does not answer statements yet: " + request.message);
	}
}
