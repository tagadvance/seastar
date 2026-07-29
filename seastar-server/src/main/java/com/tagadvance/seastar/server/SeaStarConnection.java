package com.tagadvance.seastar.server;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * The state one wire connection carries that the in-process API does not.
 *
 * <p>The keyspace {@code USE} sets is per connection on a real node, and a driver opens several
 * connections to the same node. {@link com.tagadvance.seastar.SeaStarCqlSession} has one
 * session-wide keyspace instead, so the two do not line up: a harness that runs {@code USE ks} on
 * one connection and an {@code INSERT} on another would otherwise fail, and fail intermittently.
 * The keyspace is tracked here and applied inside the funnel immediately before the statement
 * runs, which is safe precisely because the funnel serializes every request (b_plan B2).
 */
@ThreadSafe
final class SeaStarConnection {

	// Only ever touched from the funnel, but volatile all the same: a single-thread executor is
	// free to replace its thread if one dies, and the resulting visibility bug would be invisible.
	private volatile @Nullable CqlIdentifier keyspace;

	/**
	 * @return the keyspace {@code USE} set on this connection, or {@code null} if none was
	 *     selected
	 */
	@Nullable
	CqlIdentifier keyspace() {
		return keyspace;
	}

	void keyspace(final @Nullable CqlIdentifier keyspace) {
		this.keyspace = keyspace;
	}
}
