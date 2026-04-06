package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
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
			LOG.info("Adding handler to invalidate cached prepared statements on type changes");
			// TODO
//			EventExecutor adminExecutor = ctx.getNettyOptions().adminEventExecutorGroup().next();
//			ctx.getEventBus()
//				.register(TypeChangeEvent.class,
//					RunOrSchedule.on(adminExecutor, this::onTypeChanged));
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
		for (final var entry : this.cache.asMap().entrySet()) {
			try {
				final var statement = entry.getValue().get();
				if (Iterables.any(statement.getResultSetDefinitions(),
					(def) -> typeMatches(event.oldType, def.getType())) || Iterables.any(
					statement.getVariableDefinitions(),
					(def) -> typeMatches(event.oldType, def.getType()))) {

					this.cache.invalidate(entry.getKey());
					this.cache.cleanUp();
				}
			} catch (final Exception e) {
				LOG.info("Exception while invalidating prepared statement cache due to UDT change",
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
					new SeaStarCqlPrepareHandler(request, context, sessionLogPrefix).handle()
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
