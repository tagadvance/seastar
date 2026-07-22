package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.schema.DropKeyspaceStatement.Raw;

@ThreadSafe
public class DropKeyspaceHandler implements CqlHandler<Raw> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;
	private final Consumer<CqlIdentifier> setKeyspace;

	public DropKeyspaceHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace,
		final Consumer<CqlIdentifier> setKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
		this.setKeyspace = requireNonNull(setKeyspace, "setKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof Raw;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Raw raw, final Object... bindings) {
		final var coordinator = executionInfo.getCoordinator();

		final var name = Reflections.getDeclaredField(raw, "keyspaceName", String.class).orElseThrow();
		final var ifExists = Reflections.getDeclaredField(raw, "ifExists", Boolean.class).orElse(false);

		final var id = CqlIdentifier.fromInternal(name);
		if (context.getSeaStarKeyspace(id).isEmpty()) {
			if (ifExists) {
				return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
			}
			return CompletableFuture.failedStage(
				new InvalidQueryException(coordinator, "Keyspace '%s' doesn't exist".formatted(name)));
		}

		context.removeSeaStarKeyspace(id);
		if (getKeyspace.get().filter(id::equals).isPresent()) {
			setKeyspace.accept(null);
		}

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

}
