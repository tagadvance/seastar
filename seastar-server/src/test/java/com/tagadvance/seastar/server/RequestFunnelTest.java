package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.tagadvance.seastar.SeaStarCqlSession;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * b_plan B2: every request is answered on one thread that is not a Netty event loop.
 */
class RequestFunnelTest {

	private SeaStarCqlSession session;

	@BeforeEach
	void setUp() {
		session = SeaStarCqlSession.builder().build();
	}

	@AfterEach
	void tearDown() {
		session.close();
	}

	@Test
	@DisplayName("a request is answered on the funnel, not inline on the thread that decoded it")
	void testAnsweredOnTheFunnel() throws InterruptedException {
		final var answering = new ArrayBlockingQueue<String>(1);
		final var funnel = Executors.newSingleThreadExecutor(
			runnable -> new Thread(runnable, "test-funnel"));
		try {
			final var systemTables = new SystemTables("SeaStar", "datacenter1", "rack1",
				InetAddress.getLoopbackAddress(), () -> 9042);
			final var handler = new SeaStarProtocolHandler(
				new SeaStarRequestDispatcher(session, systemTables), task -> funnel.execute(() -> {
					answering.add(Thread.currentThread().getName());
					task.run();
				}));
			final var channel = new EmbeddedChannel(handler);
			channel.writeInbound(Frame.forRequest(Protocol.VERSION, 1, false, Frame.NO_PAYLOAD,
				new Query("SELECT * FROM system.local")));

			assertEquals("test-funnel", answering.poll(10, TimeUnit.SECONDS));
		} finally {
			funnel.shutdownNow();
		}
	}

	@Test
	@DisplayName("a server runs exactly one funnel thread, however many connections it is serving")
	void testOneFunnelPerServer() throws IOException {
		final var before = funnelThreads();
		try (final var server = SeaStarProtocolServer.builder().session(session).build().start()) {
			// The executor creates its thread lazily, so drive traffic through it before counting.
			try (final var first = new WireClient(server.port());
				final var second = new WireClient(server.port());
				final var third = new WireClient(server.port())) {
				first.send(ProtocolConstants.Version.V4, 1, new Startup());
				second.send(ProtocolConstants.Version.V4, 1, new Startup());
				third.send(ProtocolConstants.Version.V4, 1, new Startup());
			}

			final var added = new HashSet<>(funnelThreads());
			added.removeAll(before);

			assertEquals(1, added.size(), added.toString());
		}
	}

	/**
	 * Named rather than counted, so that a funnel another test is still shutting down cannot make
	 * this one fail.
	 */
	private static Set<String> funnelThreads() {
		return Thread.getAllStackTraces()
			.keySet()
			.stream()
			.map(Thread::getName)
			.filter(name -> name.matches("seastar-server-\\d+-funnel"))
			.collect(Collectors.toSet());
	}
}
