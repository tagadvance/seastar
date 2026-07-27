package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.NotThreadSafe;
import org.jspecify.annotations.NonNull;

/**
 * {@link SeaStarCqlSessionBuilder} is analogous to {@link CqlSessionBuilder}.
 */
@NotThreadSafe
public class SeaStarCqlSessionBuilder extends CqlSessionBuilder {

	private final List<SchemaSource> schemaSources = new ArrayList<>();

	private Clock clock = Clock.systemUTC();

	/**
	 * Sets the clock the built session stamps writes with and expires TTLs against. Defaults to
	 * {@link Clock#systemUTC()}, which is what makes a TTL behave as it does on a cluster.
	 *
	 * <p>Pass a {@link SeaStarClock} to move time by hand, so that a test asserting on expiry does
	 * not have to wait for it:
	 *
	 * <pre>{@code
	 * final var clock = SeaStarClock.now();
	 * final var session = SeaStarCqlSession.builder().withClock(clock).build();
	 * ...
	 * clock.advance(Duration.ofMinutes(2));
	 * }</pre>
	 *
	 * @param clock the clock to read the current time from
	 * @return this builder
	 */
	@NonNull
	public SeaStarCqlSessionBuilder withClock(final @NonNull Clock clock) {
		this.clock = Objects.requireNonNull(clock, "clock must not be null");

		return this;
	}

	/**
	 * Seeds the built session's schema from a CQL script. The script may contain multiple
	 * statements separated by semicolons; each is replayed through the same handler pipeline as a
	 * runtime {@code execute}, so the resulting model is identical to issuing the statements by
	 * hand.
	 *
	 * @param cql a CQL script (one or more statements)
	 * @return this builder
	 */
	@NonNull
	public SeaStarCqlSessionBuilder withSchema(final @NonNull String cql) {
		Objects.requireNonNull(cql, "cql must not be null");
		schemaSources.add(new SchemaSource("CQL string", () -> cql));

		return this;
	}

	/**
	 * Seeds the built session's schema from a {@code .cql} file, read as UTF-8.
	 *
	 * @param path the file to read
	 * @return this builder
	 * @see #withSchema(String)
	 */
	@NonNull
	public SeaStarCqlSessionBuilder withSchemaFile(final @NonNull Path path) {
		Objects.requireNonNull(path, "path must not be null");
		schemaSources.add(new SchemaSource("file " + path, () -> Files.readString(path)));

		return this;
	}

	/**
	 * Seeds the built session's schema from a {@code .cql} file, read as UTF-8.
	 *
	 * @param file the file to read
	 * @return this builder
	 * @see #withSchema(String)
	 */
	@NonNull
	public SeaStarCqlSessionBuilder withSchemaFile(final @NonNull File file) {
		Objects.requireNonNull(file, "file must not be null");

		return withSchemaFile(file.toPath());
	}

	/**
	 * Seeds the built session's schema from a classpath resource, read as UTF-8.
	 *
	 * @param resource the resource path (as passed to {@link ClassLoader#getResourceAsStream})
	 * @return this builder
	 * @see #withSchema(String)
	 */
	@NonNull
	public SeaStarCqlSessionBuilder withSchemaResource(final @NonNull String resource) {
		Objects.requireNonNull(resource, "resource must not be null");
		schemaSources.add(new SchemaSource("classpath resource " + resource,
			() -> readResource(resource)));

		return this;
	}

	/**
	 * Rejected, unlike the transport settings below: a contact point names a real address to connect
	 * to, and SeaStar has no network endpoint for it to name. Accepting one silently would tell the
	 * caller a connection target was configured when nothing of the kind exists.
	 *
	 * @throws UnsupportedOperationException always
	 */
	@Override
	@NonNull
	public CqlSessionBuilder addContactPoints(
		final @NonNull Collection<InetSocketAddress> contactPoints) {
		throw new UnsupportedOperationException();
	}

	/**
	 * @throws UnsupportedOperationException always
	 * @see #addContactPoints(Collection)
	 */
	@Override
	@NonNull
	public CqlSessionBuilder addContactPoint(final @NonNull InetSocketAddress contactPoint) {
		throw new UnsupportedOperationException();
	}

	/**
	 * @throws UnsupportedOperationException always
	 * @see #addContactPoints(Collection)
	 */
	@Override
	@NonNull
	public CqlSessionBuilder addContactEndPoints(
		final @NonNull Collection<EndPoint> contactPoints) {
		throw new UnsupportedOperationException();
	}

	/**
	 * @throws UnsupportedOperationException always
	 * @see #addContactPoints(Collection)
	 */
	@Override
	@NonNull
	public CqlSessionBuilder addContactEndPoint(final @NonNull EndPoint contactPoint) {
		throw new UnsupportedOperationException();
	}

	/**
	 * @throws UnsupportedOperationException always
	 * @see #addContactPoints(Collection)
	 */
	@Override
	@NonNull
	public CqlSessionBuilder withCloudProxyAddress(final InetSocketAddress cloudProxyAddress) {
		throw new UnsupportedOperationException();
	}

	/**
	 * @throws UnsupportedOperationException always
	 * @see #addContactPoints(Collection)
	 */
	@Override
	@NonNull
	public CqlSessionBuilder withCloudSecureConnectBundle(final @NonNull URL cloudConfigUrl) {
		throw new UnsupportedOperationException();
	}

	/**
	 * @throws UnsupportedOperationException always
	 * @see #addContactPoints(Collection)
	 */
	@Override
	@NonNull
	public CqlSessionBuilder withCloudSecureConnectBundle(
		final @NonNull InputStream cloudConfigInputStream) {
		throw new UnsupportedOperationException();
	}

	/**
	 * @throws UnsupportedOperationException always
	 * @see #addContactPoints(Collection)
	 */
	@Override
	@NonNull
	public CqlSessionBuilder withCloudSecureConnectBundle(final @NonNull Path cloudConfigPath) {
		throw new UnsupportedOperationException();
	}

	// withNodeStateListener, addNodeStateListener, withAuthProvider, withAuthCredentials,
	// withSslContext, withSslEngineFactory, withLocalDatacenter, withMetricRegistry and
	// withNodeDistanceEvaluator are not overridden: unlike a contact point, none of them name a
	// behavior SeaStar cannot provide, only one it does not need. SessionBuilder's own
	// implementation records the value and returns; accepting-and-ignoring lets a caller share
	// builder-configuration code between a real session and a SeaStar one.

	@Override
	@NonNull
	public CompletionStage<CqlSession> buildAsync() {
		return CompletableFuture.completedFuture(build());
	}

	@Override
	@NonNull
	public SeaStarCqlSession build() {
		final var programmaticArguments = programmaticArgumentsBuilder.build();
		final var configLoader = this.configLoader != null ? this.configLoader
			: defaultConfigLoader(programmaticArguments.getClassLoader());
		final var context = buildContext(configLoader, programmaticArguments);
		final var defaultConfig = configLoader.getInitialConfig().getDefaultProfile();
		if (keyspace == null && defaultConfig.isDefined(DefaultDriverOption.SESSION_KEYSPACE)) {
			keyspace = CqlIdentifier.fromCql(
				defaultConfig.getString(DefaultDriverOption.SESSION_KEYSPACE));
		}

		final var session = new SeaStarCqlSession(context, keyspace);
		applySchema(session);

		return session;
	}

	@Override
	protected SeaStarDriverContext buildContext(final DriverConfigLoader configLoader,
		final ProgrammaticArguments programmaticArguments) {
		return new VolatileDriverContext(configLoader, programmaticArguments, clock);
	}

	private void applySchema(final SeaStarCqlSession session) {
		for (final var source : schemaSources) {
			final String cql;
			try {
				cql = source.loader().load();
			} catch (final IOException | RuntimeException e) {
				throw new IllegalStateException(
					"Failed to read schema from " + source.description() + ": " + e.getMessage(), e);
			}

			for (final var statement : CqlStatements.split(cql)) {
				try {
					session.execute(statement);
				} catch (final RuntimeException e) {
					throw new IllegalStateException("Failed to execute schema statement from "
						+ source.description() + " [" + statement + "]: " + e.getMessage(), e);
				}
			}
		}
	}

	private static String readResource(final String resource) throws IOException {
		final var contextClassLoader = Thread.currentThread().getContextClassLoader();
		final var classLoader = contextClassLoader != null ? contextClassLoader
			: SeaStarCqlSessionBuilder.class.getClassLoader();
		try (final var in = classLoader.getResourceAsStream(resource)) {
			if (in == null) {
				throw new FileNotFoundException("classpath resource not found: " + resource);
			}

			return new String(in.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	@FunctionalInterface
	private interface CqlLoader {

		String load() throws IOException;

	}

	private record SchemaSource(String description, CqlLoader loader) {

	}

}
