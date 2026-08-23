package com.tagadvance.seastar.handlers;

/**
 * Thrown when the {@code cassandra-all} parse tree does not look the way {@link Reflections}
 * expects - a class or field missing, or a field holding the wrong type - which means the pinned
 * version was bumped without the handlers being revisited.
 */
public class ReflectionException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ReflectionException(final String message) {
		super(message);
	}

	public ReflectionException(final String message, final Throwable cause) {
		super(message, cause);
	}

}
