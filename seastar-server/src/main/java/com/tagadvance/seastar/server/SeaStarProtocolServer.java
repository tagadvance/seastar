package com.tagadvance.seastar.server;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.driver.internal.core.protocol.FrameDecoder;
import com.datastax.oss.driver.internal.core.protocol.FrameEncoder;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * Serves a {@link CqlSession} over Cassandra's native protocol, so a client that cannot swap its
 * own session for SeaStar's in-process can still talk to one.
 *
 * <pre>{@code
 * final var session = SeaStarCqlSession.builder().build();
 * try (final var server = SeaStarProtocolServer.builder().session(session).build().start()) {
 *     final var contactPoint = new InetSocketAddress(InetAddress.getLoopbackAddress(),
 *         server.port());
 *     ...
 * }
 * }</pre>
 *
 * <p>The defaults are the ones a test wants: an ephemeral port, bound to the loopback address
 * only. A test library that grabbed 9042 on every interface would collide with a developer's real
 * local Cassandra and be blamed for it, so the port a harness needs is one it has to ask for.
 *
 * <p>{@link #close()} does not close the wrapped session. Ownership stays with whoever built it,
 * and a caller may well be using the same session in-process at the same time.
 *
 * <p><strong>Protocol v4 only.</strong> A request at any other version is answered with the
 * {@code PROTOCOL_ERROR} that makes a driver retry one version lower, which is what lets a driver
 * left on its own v5 default reach this server at all.
 */
@ThreadSafe
public final class SeaStarProtocolServer implements AutoCloseable {

	/**
	 * The protocol allows 256 MiB. This is a fake serving a loopback socket; 64 MiB is an honest
	 * ceiling and an oversized frame fails rather than being silently truncated.
	 */
	private static final int MAX_FRAME_LENGTH = 64 * 1024 * 1024;

	private static final AtomicInteger SERVER_COUNT = new AtomicInteger();

	private final CqlSession session;
	private final InetAddress bindAddress;
	private final int requestedPort;
	private final int id = SERVER_COUNT.incrementAndGet();

	private final AtomicBoolean started = new AtomicBoolean();
	private final AtomicBoolean closed = new AtomicBoolean();

	private volatile @Nullable Running running;

	private SeaStarProtocolServer(final Builder builder) {
		this.session = requireNonNull(builder.session,
			"a session is required to serve one; call Builder#session");
		this.bindAddress = builder.bindAddress;
		this.requestedPort = builder.port;
	}

	/**
	 * @return a builder for a server that is not yet bound
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Binds the socket. Returns once the bind has completed, so {@link #port()} is readable
	 * immediately afterwards.
	 *
	 * @return this server, so that it can be built and started in a try-with-resources header
	 * @throws IllegalStateException if this server has already been started, or if the bind failed
	 */
	public SeaStarProtocolServer start() {
		if (!started.compareAndSet(false, true)) {
			throw new IllegalStateException("this server has already been started");
		}

		// One acceptor and one I/O thread: every request is answered on the funnel (b_plan B2), so
		// extra event loops would only add threads to start and to shut down. Startup cost is a
		// goal-2 concern for a library that means to beat TestContainers.
		final var acceptors = new NioEventLoopGroup(1, threads("accept"));
		final var workers = new NioEventLoopGroup(1, threads("io"));
		final var funnel = Executors.newSingleThreadExecutor(threads("funnel"));
		final var connections = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

		// FrameCodec and ByteBufPrimitiveCodec are both stateless and thread-safe, and FrameEncoder
		// is @Sharable, so all three are built once and shared by every connection. FrameDecoder
		// extends LengthFieldBasedFrameDecoder and holds reassembly state, so it is per channel.
		final var frameCodec = FrameCodec.defaultServer(
			new ByteBufPrimitiveCodec(ByteBufAllocator.DEFAULT), Compressor.none());
		final var encoder = new FrameEncoder(frameCodec, MAX_FRAME_LENGTH);
		final var dispatcher = new SeaStarRequestDispatcher(session);

		final var bootstrap = new ServerBootstrap().group(acceptors, workers)
			.channel(NioServerSocketChannel.class)
			.childOption(ChannelOption.TCP_NODELAY, true)
			.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(final SocketChannel channel) {
					connections.add(channel);
					// The version gate goes ahead of the decoder because the versions worth turning
					// away include ones the decoder cannot read at all - see ProtocolVersionGate.
					channel.pipeline()
						.addLast("frameEncoder", encoder)
						.addLast("versionGate", new ProtocolVersionGate())
						.addLast("frameDecoder", new FrameDecoder(frameCodec, MAX_FRAME_LENGTH))
						.addLast("dispatch", new SeaStarProtocolHandler(dispatcher, funnel));
				}
			});

		final Channel serverChannel;
		try {
			serverChannel = bootstrap.bind(bindAddress, requestedPort).sync().channel();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			shutdown(acceptors, workers, funnel);

			throw new IllegalStateException("interrupted while binding " + describeBind(), e);
		} catch (final Exception e) {
			// ChannelFuture#sync rethrows the bind failure itself, BindException included.
			shutdown(acceptors, workers, funnel);

			throw new IllegalStateException("failed to bind " + describeBind(), e);
		}

		running = new Running(acceptors, workers, funnel, serverChannel, connections);

		return this;
	}

	/**
	 * @return the port this server is bound to, which is the whole point of allowing port 0
	 * @throws IllegalStateException if this server has not been started
	 */
	public int port() {
		final var current = running;
		if (current == null) {
			throw new IllegalStateException(
				"the bound port is not known until the server has been started");
		}

		return ((InetSocketAddress) current.serverChannel.localAddress()).getPort();
	}

	/**
	 * @return the address this server is bound to
	 */
	public InetAddress bindAddress() {
		return bindAddress;
	}

	/**
	 * Stops accepting, closes every open connection, and shuts down the event loops and the
	 * funnel. Idempotent, and safe on a server that was built but never started.
	 *
	 * <p>The session this server was built with is left open; it belongs to the caller.
	 */
	@Override
	public void close() {
		if (!closed.compareAndSet(false, true)) {
			return;
		}

		final var current = running;
		if (current == null) {
			return;
		}

		current.serverChannel.close().awaitUninterruptibly();
		current.connections.close().awaitUninterruptibly();
		shutdown(current.acceptors, current.workers, current.funnel);
	}

	@Override
	public String toString() {
		final var current = running;

		return "SeaStarProtocolServer[" + (current == null ? describeBind() + ", not started"
			: bindAddress.getHostAddress() + ":" + port()) + "]";
	}

	private String describeBind() {
		return bindAddress.getHostAddress() + ":" + requestedPort;
	}

	private ThreadFactory threads(final String role) {
		// Daemon threads throughout: a server somebody forgot to close must not hold a test JVM up.
		return runnable -> {
			final var thread = new Thread(runnable, "seastar-server-" + id + "-" + role);
			thread.setDaemon(true);

			return thread;
		};
	}

	private static void shutdown(final EventLoopGroup acceptors, final EventLoopGroup workers,
		final ExecutorService funnel) {
		// Drain what the funnel already accepted, then stop waiting for it. The default two-second
		// quiet period on the event loops would make every close() in a test suite cost two
		// seconds, so it is explicitly zero.
		funnel.shutdown();
		try {
			if (!funnel.awaitTermination(2, TimeUnit.SECONDS)) {
				funnel.shutdownNow();
			}
		} catch (final InterruptedException e) {
			funnel.shutdownNow();
			Thread.currentThread().interrupt();
		}

		acceptors.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
		workers.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
	}

	/**
	 * Everything {@link #start()} creates, in one field, so that {@link #close()} sees either all
	 * of it or none of it.
	 */
	private record Running(EventLoopGroup acceptors, EventLoopGroup workers, ExecutorService funnel,
		Channel serverChannel, ChannelGroup connections) {

	}

	/**
	 * Collects what a {@link SeaStarProtocolServer} needs before it binds. Only the session is
	 * required.
	 */
	@NotThreadSafe
	public static final class Builder {

		private @Nullable CqlSession session;
		private InetAddress bindAddress = InetAddress.getLoopbackAddress();
		private int port;

		private Builder() {

		}

		/**
		 * Sets the session every request is answered from. Required.
		 *
		 * @param session the session to serve
		 * @return this builder
		 */
		public Builder session(final CqlSession session) {
			this.session = requireNonNull(session, "session must not be null");

			return this;
		}

		/**
		 * Sets the port to bind. Defaults to {@code 0}, an ephemeral port chosen by the operating
		 * system and readable afterwards from {@link SeaStarProtocolServer#port()}.
		 *
		 * @param port the port to bind, or {@code 0} for an ephemeral one
		 * @return this builder
		 */
		public Builder port(final int port) {
			if (port < 0 || port > 0xFFFF) {
				throw new IllegalArgumentException("port must be between 0 and 65535, got " + port);
			}
			this.port = port;

			return this;
		}

		/**
		 * Sets the address to bind. Defaults to the loopback address; a server that listened on
		 * every interface by default would be a surprise nobody asked a test library for.
		 *
		 * @param bindAddress the address to bind
		 * @return this builder
		 */
		public Builder bindAddress(final InetAddress bindAddress) {
			this.bindAddress = requireNonNull(bindAddress, "bindAddress must not be null");

			return this;
		}

		/**
		 * @return a server that is configured but not yet bound; call
		 *     {@link SeaStarProtocolServer#start()}
		 */
		public SeaStarProtocolServer build() {
			return new SeaStarProtocolServer(this);
		}
	}
}
