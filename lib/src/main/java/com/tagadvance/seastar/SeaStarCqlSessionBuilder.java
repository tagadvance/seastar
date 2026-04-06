package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.net.ssl.SSLContext;
import net.jcip.annotations.NotThreadSafe;
import org.jspecify.annotations.NonNull;

/**
 * {@link SeaStarCqlSessionBuilder} is analogous to {@link CqlSessionBuilder}.
 */
@NotThreadSafe
public class SeaStarCqlSessionBuilder extends CqlSessionBuilder {

	// TODO: populate metadata with ClassPathSchema, File Schema, ResourceSchema, CQL schema

	@Override
	@NonNull
	public CqlSessionBuilder addContactPoints(
		final @NonNull Collection<InetSocketAddress> contactPoints) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder addContactPoint(final @NonNull InetSocketAddress contactPoint) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder addContactEndPoints(
		final @NonNull Collection<EndPoint> contactPoints) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder addContactEndPoint(final @NonNull EndPoint contactPoint) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder withNodeStateListener(final NodeStateListener nodeStateListener) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder addNodeStateListener(
		final @NonNull NodeStateListener nodeStateListener) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder withAuthProvider(final AuthProvider authProvider) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder withAuthCredentials(final @NonNull String username,
		final @NonNull String password) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder withAuthCredentials(final @NonNull String username,
		final @NonNull String password, final @NonNull String authorizationId) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder withSslContext(final SSLContext sslContext) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CqlSessionBuilder withLocalDatacenter(final @NonNull String profileName,
		final @NonNull String localDatacenter) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CqlSessionBuilder withLocalDatacenter(final @NonNull String localDatacenter) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder withMetricRegistry(final Object metricRegistry) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder withCloudProxyAddress(final InetSocketAddress cloudProxyAddress) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder withCloudSecureConnectBundle(final @NonNull URL cloudConfigUrl) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder withCloudSecureConnectBundle(
		final @NonNull InputStream cloudConfigInputStream) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder withCloudSecureConnectBundle(final @NonNull Path cloudConfigPath) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder withNodeDistanceEvaluator(
		final @NonNull NodeDistanceEvaluator nodeDistanceEvaluator) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder withNodeDistanceEvaluator(final @NonNull String profileName,
		final @NonNull NodeDistanceEvaluator nodeDistanceEvaluator) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CqlSessionBuilder withSslEngineFactory(final SslEngineFactory sslEngineFactory) {
		throw new UnsupportedOperationException();
	}

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

		return new SeaStarCqlSession(context, keyspace);
	}

	@Override
	protected SeaStarDriverContext buildContext(final DriverConfigLoader configLoader,
		final ProgrammaticArguments programmaticArguments) {
		return new VolatileDriverContext(configLoader, programmaticArguments);
	}

}
