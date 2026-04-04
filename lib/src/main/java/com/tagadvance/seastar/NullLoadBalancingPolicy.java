package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.tagadvance.tools.Queues;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import org.jspecify.annotations.NonNull;

/**
 * A {@link LoadBalancingPolicy} that does nothing.
 */
public final class NullLoadBalancingPolicy implements LoadBalancingPolicy {

	@Override
	public void init(final @NonNull Map<UUID, Node> nodes,
		final @NonNull DistanceReporter distanceReporter) {
		// do nothing
	}

	@Override
	@NonNull
	public Queue<Node> newQueryPlan(final Request request, final Session session) {
		return Queues.unmodifiableQueue(new LinkedList<>());
	}

	@Override
	public void onAdd(final @NonNull Node node) {
		// do nothing
	}

	@Override
	public void onUp(final @NonNull Node node) {
		// do nothing
	}

	@Override
	public void onDown(final @NonNull Node node) {
		// do nothing
	}

	@Override
	public void onRemove(final @NonNull Node node) {
		// do nothing
	}

	@Override
	public void close() {
		// do nothing
	}

}
