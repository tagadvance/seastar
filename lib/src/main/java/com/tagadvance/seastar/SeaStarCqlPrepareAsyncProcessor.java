package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TableChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TypeChangeEvent;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.base.Functions;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SeaStarCqlPrepareAsyncProcessor} is analogous to {@link CqlPrepareAsyncProcessor}.
 */
@ThreadSafe
public class SeaStarCqlPrepareAsyncProcessor implements
	SeaStarRequestProcessor<PrepareRequest, CompletionStage<SeaStarPreparedStatement>> {

	private static final Logger LOG = LoggerFactory.getLogger(
		SeaStarCqlPrepareAsyncProcessor.class);

	protected final Cache<PrepareRequest, CompletableFuture<SeaStarPreparedStatement>> cache;

	public SeaStarCqlPrepareAsyncProcessor() {
		this(Optional.empty());
	}

	public SeaStarCqlPrepareAsyncProcessor(
		final @NonNull Optional<? extends SeaStarDriverContext> context) {
		this(context, Functions.identity());
	}

	protected SeaStarCqlPrepareAsyncProcessor(
		final Optional<? extends SeaStarDriverContext> context,
		Function<CacheBuilder<Object, Object>, CacheBuilder<Object, Object>> decorator) {

		CacheBuilder<Object, Object> baseCache = CacheBuilder.newBuilder().weakValues();
		this.cache = decorator.apply(baseCache).build();
		context.ifPresent((ctx) -> {
			LOG.debug("Adding handlers to invalidate cached prepared statements on schema changes");
			// VolatileDriverContext reuses the driver's event bus, so registering the same
			// TypeChangeEvent listener the real driver uses evicts cached prepared statements whose
			// bind or result variables reference a UDT once something (e.g. ALTER TYPE) fires the event.
			final var eventBus = ((InternalDriverContext) ctx).getEventBus();
			eventBus.register(TypeChangeEvent.class, this::onTypeChanged);
			// ALTER TABLE has no driver-side equivalent to lean on, but the event is the driver's own
			// and the reason to listen is the same: a statement prepared against the old column list
			// would keep answering with it.
			eventBus.register(TableChangeEvent.class, this::onTableChanged);
		});
	}

	private static boolean typeMatches(final UserDefinedType oldType, final DataType typeToCheck) {
		return switch (typeToCheck.getProtocolCode()) {
			case ProtocolConstants.DataType.UDT -> {
				final UserDefinedType udtType = (UserDefinedType) typeToCheck;

				yield udtType.equals(oldType) || Iterables.any(udtType.getFieldTypes(),
					(testType) -> typeMatches(oldType, testType));
			}
			case ProtocolConstants.DataType.LIST -> {
				ListType listType = (ListType) typeToCheck;

				yield typeMatches(oldType, listType.getElementType());
			}
			case ProtocolConstants.DataType.SET -> {
				SetType setType = (SetType) typeToCheck;

				yield typeMatches(oldType, setType.getElementType());
			}
			case ProtocolConstants.DataType.MAP -> {
				MapType mapType = (MapType) typeToCheck;

				yield typeMatches(oldType, mapType.getKeyType()) || typeMatches(oldType,
					mapType.getValueType());
			}
			case ProtocolConstants.DataType.TUPLE -> {
				TupleType tupleType = (TupleType) typeToCheck;

				yield Iterables.any(tupleType.getComponentTypes(),
					(testType) -> typeMatches(oldType, testType));
			}
			default -> false;
		};
	}

	private void onTypeChanged(final TypeChangeEvent event) {
		final var type = event.oldType != null ? event.oldType : event.newType;
		invalidateIf(statement -> Iterables.any(statement.getResultSetDefinitions(),
				(def) -> typeMatches(type, def.getType())) || Iterables.any(
				statement.getVariableDefinitions(), (def) -> typeMatches(type, def.getType())),
			"UDT change");
	}

	/**
	 * Evicts the prepared statements whose bind or result variables name the changed table, so that
	 * the next prepare re-resolves them against the current columns.
	 *
	 * <p>A statement that names no columns at all - an {@code INSERT} written entirely with literals,
	 * say - has nothing to match on and survives, which costs nothing: it had no column list to go
	 * stale.
	 */
	private void onTableChanged(final TableChangeEvent event) {
		final var table = event.oldTable != null ? event.oldTable : event.newTable;
		invalidateIf(
			statement -> references(statement.getResultSetDefinitions(), table) || references(
				statement.getVariableDefinitions(), table), "table change");
	}

	private static boolean references(final ColumnDefinitions definitions,
		final TableMetadata table) {
		return Iterables.any(definitions,
			(def) -> table.getKeyspace().equals(def.getKeyspace()) && table.getName()
				.equals(def.getTable()));
	}

	private void invalidateIf(final Predicate<SeaStarPreparedStatement> stale, final String reason) {
		for (final var entry : this.cache.asMap().entrySet()) {
			try {
				if (stale.test(entry.getValue().get())) {
					this.cache.invalidate(entry.getKey());
					this.cache.cleanUp();
				}
			} catch (final Exception e) {
				LOG.info("Exception while invalidating prepared statement cache due to {}", reason,
					e);
			}
		}
	}

	@Override
	public boolean canProcess(final Request request, final GenericType<?> resultType) {
		return request instanceof PrepareRequest && resultType.equals(PrepareRequest.ASYNC);
	}

	@Override
	public CompletionStage<SeaStarPreparedStatement> process(final PrepareRequest request,
		final SeaStarCqlSession session, final SeaStarDriverContext context,
		final String sessionLogPrefix) {
		try {
			CompletableFuture<SeaStarPreparedStatement> result = cache.getIfPresent(request);
			if (result == null) {
				final CompletableFuture<SeaStarPreparedStatement> mine = new CompletableFuture<>();
				result = cache.get(request, () -> mine);
				if (result == mine) {
					new SeaStarCqlPrepareHandler(request, context, sessionLogPrefix,
						session.getKeyspace().orElse(null)).handle()
						.whenComplete((preparedStatement, error) -> {
							if (error != null) {
								mine.completeExceptionally(error);
								cache.invalidate(
									request); // Make sure failure isn't cached indefinitely
							} else {
								mine.complete(preparedStatement);
							}
						});
				}
			}

			// Return a defensive copy. So if a client cancels its request, the cache won't be impacted
			// nor a potential concurrent request.
			return result.copy();
		} catch (final ExecutionException e) {
			return CompletableFutures.failedFuture(e.getCause());
		}
	}

	@Override
	public CompletionStage<SeaStarPreparedStatement> newFailure(final RuntimeException error) {
		return CompletableFutures.failedFuture(error);
	}

	public Cache<PrepareRequest, CompletableFuture<SeaStarPreparedStatement>> getCache() {
		return cache;
	}

}
