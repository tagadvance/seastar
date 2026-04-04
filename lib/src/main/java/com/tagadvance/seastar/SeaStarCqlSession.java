package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.errorprone.annotations.ThreadSafe;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

@ThreadSafe
public class SeaStarCqlSession implements CqlSession {

	private final SeaStarDriverContext context;
	private final AtomicReference<CqlIdentifier> keyspace = new AtomicReference<>();
	private final SeaStarRequestProcessorRegistry registry;

	public SeaStarCqlSession(final @NonNull SeaStarDriverContext context,
		final @Nullable CqlIdentifier keyspace) {
		this.context = requireNonNull(context, "context must not be null");
		this.keyspace.set(keyspace);
		this.registry = context.getSeaStarRequestProcessorRegistry();
	}

	@Override
	@NonNull
	public String getName() {
		return context.getSessionName();
	}

	@Override
	@NonNull
	public Metadata getMetadata() {
		return context.node;
	}

	@Override
	public boolean isSchemaMetadataEnabled() {
		return true;
	}

	@Override
	@NonNull
	public CompletionStage<Metadata> setSchemaMetadataEnabled(final Boolean newValue) {
		return CompletableFuture.completedFuture(context.node);
	}

	@Override
	@NonNull
	public CompletionStage<Metadata> refreshSchemaAsync() {
		// TODO: perform refresh
		return CompletableFuture.completedFuture(context.node);
	}

	@Override
	@NonNull
	public CompletionStage<Boolean> checkSchemaAgreementAsync() {
		return CompletableFuture.completedFuture(true);
	}

	@Override
	@NonNull
	public SeaStarDriverContext getContext() {
		return context;
	}

	@Override
	@NonNull
	public Optional<CqlIdentifier> getKeyspace() {
		return Optional.of(keyspace).map(AtomicReference::get);
	}

	@Override
	@NonNull
	public Optional<Metrics> getMetrics() {
		throw new UnsupportedOperationException("Metrics are not supported in SeaStarCqlSession");
	}

	@Override
	public <RequestT extends Request, ResultT> ResultT execute(@NonNull final RequestT request,
		final @NonNull GenericType<ResultT> resultType) {
		final var processor = registry.processorFor(request, resultType);

		return processor.process(request, this, context, context.getSessionName());
	}

	@Override
	@NonNull
	public CompletionStage<Void> closeFuture() {
		return CompletableFuture.completedStage(null);
	}

	@Override
	@NonNull
	public CompletionStage<Void> closeAsync() {
		return CompletableFuture.completedStage(null);
	}

	@Override
	@NonNull
	public CompletionStage<Void> forceCloseAsync() {
		return CompletableFuture.completedStage(null);
	}

	@NonNull
	public static SeaStarCqlSessionBuilder builder() {
		return new SeaStarCqlSessionBuilder();
	}

}
