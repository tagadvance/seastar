package com.tagadvance.seastar.server;

import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.error.AlreadyExists;
import com.tagadvance.seastar.handlers.CqlStatementSummary;
import java.util.concurrent.CompletionException;
import org.jspecify.annotations.Nullable;

/**
 * The inverse of what the core's handlers do: they throw the driver exception a live cluster would
 * have produced, and the wire wants the error code that produces it again at the other end.
 *
 * <p>This is what makes an assertion survive the move onto a socket. A test written against
 * in-process SeaStar that expects an {@link InvalidQueryException} keeps passing over the wire,
 * because the driver rebuilds the same type from the code. It matters most for the statements
 * SeaStar does not implement: those are meant to reach a caller as a named refusal quoting the
 * query, and a {@code SERVER_ERROR} would read as SeaStar having fallen over instead.
 *
 * <p>Exception <em>messages</em> travel as they are. SeaStar's wording is its own rather than
 * Cassandra's byte for byte, and that is a standing decision; the wire preserves whatever the core
 * said. The one exception is {@code ALREADY_EXISTS}, which the driver rebuilds from the keyspace
 * and object names rather than from the message it was sent.
 */
final class Failures {

	private Failures() {

	}

	/**
	 * @param failure the exception a request failed with
	 * @param summary what the statement was, used only to name the keyspace and object of an
	 *                {@code ALREADY_EXISTS}, or {@code null} where the statement is not known
	 * @return the error to answer with
	 */
	static Error of(final Throwable failure, final @Nullable CqlStatementSummary summary) {
		// executeAsync answers on the calling thread, so a failure arrives wrapped by
		// CompletableFuture#join rather than thrown; the cause is the exception a handler raised.
		final var cause = failure instanceof CompletionException && failure.getCause() != null
			? failure.getCause() : failure;
		final var message = String.valueOf(cause.getMessage());

		// AlreadyExistsException extends QueryValidationException, as does SyntaxError, so both have
		// to be asked about before InvalidQueryException.
		if (cause instanceof AlreadyExistsException) {
			return alreadyExists(message, summary);
		}
		if (cause instanceof SyntaxError) {
			return new Error(ProtocolConstants.ErrorCode.SYNTAX_ERROR, message);
		}
		if (cause instanceof InvalidQueryException) {
			return new Error(ProtocolConstants.ErrorCode.INVALID, message);
		}
		if (cause instanceof UnauthorizedException) {
			return new Error(ProtocolConstants.ErrorCode.UNAUTHORIZED, message);
		}
		// The support matrix's "deliberately unimplemented" travels this way. A live node reports a
		// feature it has switched off as INVALID, so a feature SeaStar has not built reports the same.
		if (cause instanceof UnsupportedOperationException) {
			return new Error(ProtocolConstants.ErrorCode.INVALID, message);
		}

		return new Error(ProtocolConstants.ErrorCode.SERVER_ERROR, String.valueOf(cause));
	}

	/**
	 * {@code ALREADY_EXISTS} carries the keyspace and object beyond the message, and the driver
	 * rebuilds the exception - message included - from those two rather than from what it was sent.
	 * The names come from the statement rather than from the exception because the driver's
	 * {@link AlreadyExistsException} keeps both fields private with no accessor.
	 */
	private static Error alreadyExists(final String message,
		final @Nullable CqlStatementSummary summary) {
		if (summary instanceof CqlStatementSummary.SchemaChanged changed) {
			return new AlreadyExists(message, changed.keyspace(),
				changed.object() == null ? "" : changed.object());
		}

		return new AlreadyExists(message, "", "");
	}

}
