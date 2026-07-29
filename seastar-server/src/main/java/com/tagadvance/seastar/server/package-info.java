/**
 * A native-protocol listener that serves an in-memory {@code SeaStarCqlSession} over a socket, for
 * clients that cannot swap their {@code CqlSession} for SeaStar's in-process.
 *
 * <p>The dependency runs one way only: this module is built against {@code :seastar}, and
 * {@code :seastar} knows nothing about it. Anything the listener needs from the core is a
 * deliberate addition to the core's public API rather than a visibility bump made in passing.
 */
@NullMarked
package com.tagadvance.seastar.server;

import org.jspecify.annotations.NullMarked;
