package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.tagadvance.seastar.handlers.AlterKeyspaceHandler;
import com.tagadvance.seastar.handlers.AlterTableHandler;
import com.tagadvance.seastar.handlers.AlterTypeHandler;
import com.tagadvance.seastar.handlers.BatchHandler;
import com.tagadvance.seastar.handlers.CqlHandlerRegistry;
import com.tagadvance.seastar.handlers.CreateIndexHandler;
import com.tagadvance.seastar.handlers.CreateKeyspaceHandler;
import com.tagadvance.seastar.handlers.CreateTableHandler;
import com.tagadvance.seastar.handlers.CreateTypeHandler;
import com.tagadvance.seastar.handlers.DeleteHandler;
import com.tagadvance.seastar.handlers.DropIndexHandler;
import com.tagadvance.seastar.handlers.DropKeyspaceHandler;
import com.tagadvance.seastar.handlers.DropTableHandler;
import com.tagadvance.seastar.handlers.DropTypeHandler;
import com.tagadvance.seastar.handlers.InsertHandler;
import com.tagadvance.seastar.handlers.SelectHandler;
import com.tagadvance.seastar.handlers.TruncateHandler;
import com.tagadvance.seastar.handlers.UpdateHandler;
import com.tagadvance.seastar.handlers.UseKeyspaceHandler;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Safe for concurrent use. A statement is answered entirely on the calling thread - SeaStar never
 * spawns one of its own - so an async result is already complete by the time it is returned;
 * {@code whenComplete} and friends run inline rather than on a callback thread.
 */
@ThreadSafe
public class SeaStarCqlSession implements CqlSession {

	private final SeaStarDriverContext context;
	private final AtomicReference<CqlIdentifier> keyspace = new AtomicReference<>();
	private final SeaStarRequestProcessorRegistry registry;
	private final CqlHandlerRegistry handlerRegistry;
	private final AtomicBoolean closed = new AtomicBoolean();
	private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

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
			new AlterKeyspaceHandler(),
			new UseKeyspaceHandler(this::setKeyspace),
			new CreateTypeHandler(this::getKeyspace),
			new AlterTypeHandler(this::getKeyspace),
			new DropTypeHandler(this::getKeyspace),
			new CreateTableHandler(this::getKeyspace),
			new AlterTableHandler(this::getKeyspace),
			new CreateIndexHandler(this::getKeyspace),
			new DropIndexHandler(this::getKeyspace),
			new DropTableHandler(this::getKeyspace),
			new DropKeyspaceHandler(this::getKeyspace, this::setKeyspace),
			new InsertHandler(this::getKeyspace),
			new UpdateHandler(this::getKeyspace),
			new DeleteHandler(this::getKeyspace),
			new TruncateHandler(this::getKeyspace),
			new BatchHandler(this::handlerRegistry),
			new SelectHandler(this::getKeyspace));
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
		return Optional.ofNullable(keyspace.get());
	}

	/**
	 * Selects the keyspace an unqualified statement resolves against, exactly as {@code USE} does but
	 * without a statement, and without requiring that the keyspace exists.
	 *
	 * <p>This is public for one reason: Cassandra's native protocol keeps the selected keyspace
	 * <em>per connection</em>, and a driver opens several connections to the same node, while a
	 * session has only one. A server serving this session over the wire has to point it at the
	 * keyspace the connection in hand selected, and it has to be able to point it at nothing.
	 * {@code USE} cannot express "nothing", and it validates - where a real node remembers the name
	 * a connection asked for whether or not the keyspace is still there, so that dropping a keyspace
	 * and recreating it leaves the connection working.
	 *
	 * @param identifier the keyspace to select, or {@code null} to select none
	 */
	public void setKeyspace(final @Nullable CqlIdentifier identifier) {
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

	/**
	 * Mirrors {@code DefaultSession#execute}: once the session is closed, the request is not
	 * processed and the failure is reported through the processor, so a synchronous call throws
	 * {@link IllegalStateException} while an asynchronous one returns a failed stage.
	 */
	@Override
	public <RequestT extends Request, ResultT> ResultT execute(@NonNull final RequestT request,
		final @NonNull GenericType<ResultT> resultType) {
		final var processor = registry.processorFor(request, resultType);

		return closed.get() ? processor.newFailure(new IllegalStateException("Session is closed"))
			: processor.process(request, this, context, context.getSessionName());
	}

	@Override
	@NonNull
	public CompletionStage<Void> closeFuture() {
		return closeFuture;
	}

	/**
	 * Closes the session. The first call completes {@link #closeFuture()}; subsequent calls are
	 * no-ops that return the same stage.
	 *
	 * <p>The keyspaces are kept, matching the real driver, which leaves {@link #getMetadata()}
	 * readable after close. Nothing outside the session references them, so they are collected with
	 * it; an earlier design dropped them here to make a leaked session loud, and was reversed for
	 * fidelity.
	 */
	@Override
	@NonNull
	public CompletionStage<Void> closeAsync() {
		if (closed.compareAndSet(false, true)) {
			closeFuture.complete(null);
		}

		return closeFuture;
	}

	/**
	 * SeaStar runs every request on the calling thread and completes it before returning, so there is
	 * never an in-flight request to abort; a forced close is an ordinary close.
	 */
	@Override
	@NonNull
	public CompletionStage<Void> forceCloseAsync() {
		return closeAsync();
	}

	@NonNull
	public static SeaStarCqlSessionBuilder builder() {
		return new SeaStarCqlSessionBuilder();
	}

}
