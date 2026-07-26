package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.errorprone.annotations.ThreadSafe;
import com.tagadvance.seastar.handlers.AlterTypeHandler;
import com.tagadvance.seastar.handlers.BatchHandler;
import com.tagadvance.seastar.handlers.CqlHandlerRegistry;
import com.tagadvance.seastar.handlers.CreateIndexHandler;
import com.tagadvance.seastar.handlers.CreateKeyspaceHandler;
import com.tagadvance.seastar.handlers.CreateTableHandler;
import com.tagadvance.seastar.handlers.CreateTypeHandler;
import com.tagadvance.seastar.handlers.DeleteHandler;
import com.tagadvance.seastar.handlers.DropKeyspaceHandler;
import com.tagadvance.seastar.handlers.DropTableHandler;
import com.tagadvance.seastar.handlers.InsertHandler;
import com.tagadvance.seastar.handlers.SelectHandler;
import com.tagadvance.seastar.handlers.TruncateHandler;
import com.tagadvance.seastar.handlers.UpdateHandler;
import com.tagadvance.seastar.handlers.UseKeyspaceHandler;
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
	private final CqlHandlerRegistry handlerRegistry;

	public SeaStarCqlSession(final @NonNull SeaStarDriverContext context,
		final @Nullable CqlIdentifier keyspace) {
		this.context = requireNonNull(context, "context must not be null");
		this.keyspace.set(keyspace);
		this.registry = buildSeaStarRequestProcessorRegistry();
		this.handlerRegistry = buildHandlerRegistry();
	}

	// Built once and shared across every request handler; the handlers are stateless and thread-safe
	// (they hold only this session's keyspace callbacks), so a single immutable registry is reused
	// rather than reallocating all handlers per query. This is the single place to register a new
	// statement handler.
	private CqlHandlerRegistry buildHandlerRegistry() {
		return new CqlHandlerRegistry(context.getSessionName(),
			new CreateKeyspaceHandler(),
			new UseKeyspaceHandler(this::setKeyspace),
			new CreateTypeHandler(this::getKeyspace),
			new AlterTypeHandler(this::getKeyspace),
			new CreateTableHandler(this::getKeyspace),
			new CreateIndexHandler(this::getKeyspace),
			new DropTableHandler(this::getKeyspace),
			new DropKeyspaceHandler(this::getKeyspace, this::setKeyspace),
			new InsertHandler(this::getKeyspace),
			new UpdateHandler(this::getKeyspace),
			new DeleteHandler(this::getKeyspace),
			new TruncateHandler(this::getKeyspace),
			new BatchHandler(this::handlerRegistry),
			new SelectHandler());
	}

	CqlHandlerRegistry handlerRegistry() {
		return handlerRegistry;
	}

	private SeaStarRequestProcessorRegistry buildSeaStarRequestProcessorRegistry() {
		final var processors = SeaStarBuiltInRequestProcessors.createDefaultProcessors(context)
			.toArray(SeaStarRequestProcessor[]::new);

		return new SeaStarRequestProcessorRegistry(getName(), processors);
	}

	@Override
	@NonNull
	public String getName() {
		return context.getSessionName();
	}

	@Override
	@NonNull
	public Metadata getMetadata() {
		return context;
	}

	@Override
	public boolean isSchemaMetadataEnabled() {
		return true;
	}

	@Override
	@NonNull
	public CompletionStage<Metadata> setSchemaMetadataEnabled(final Boolean newValue) {
		return CompletableFuture.completedFuture(context);
	}

	/**
	 * Returns the live metadata without refetching. SeaStar's in-memory {@code Volatile*} model is
	 * always current and authoritative, and the context is itself the {@link Metadata}, derived
	 * directly from that model on every access (for example {@code getKeyspaces()} reads the live
	 * keyspace map). There is no remote schema to pull and no cached or derived view to recompute,
	 * so a no-op returning the live context is the correct implementation, not a stub.
	 */
	@Override
	@NonNull
	public CompletionStage<Metadata> refreshSchemaAsync() {
		return CompletableFuture.completedFuture(context);
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

	void setKeyspace(final CqlIdentifier identifier) {
		keyspace.set(identifier);
	}

	/**
	 * SeaStar collects no metrics. The driver's own contract for this method is "empty if metrics are
	 * disabled", which is exactly SeaStar's situation, so client code that logs metrics when they are
	 * available keeps working.
	 *
	 * @return {@link Optional#empty()}, always
	 */
	@Override
	@NonNull
	public Optional<Metrics> getMetrics() {
		return Optional.empty();
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
