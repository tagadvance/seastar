package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.tagadvance.seastar.handlers.CqlStatementSummary.Change;
import com.tagadvance.seastar.handlers.CqlStatementSummary.KeyspaceSelected;
import com.tagadvance.seastar.handlers.CqlStatementSummary.Result;
import com.tagadvance.seastar.handlers.CqlStatementSummary.SchemaChanged;
import com.tagadvance.seastar.handlers.CqlStatementSummary.Target;
import java.util.Optional;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.cql3.statements.schema.AlterKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.AlterTableStatement;
import org.apache.cassandra.cql3.statements.schema.AlterTypeStatement;
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.schema.DropIndexStatement;
import org.apache.cassandra.cql3.statements.schema.DropKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.DropTableStatement;
import org.apache.cassandra.cql3.statements.schema.DropTypeStatement;
import org.jspecify.annotations.Nullable;

/**
 * Reads a parse tree far enough to fill in a {@link CqlStatementSummary}.
 *
 * <p>Separate from the summary itself so that the public type names only driver classes. A private
 * method taking a {@code CQLStatement.Raw} would still put {@code org.apache.cassandra} in the
 * interface's class file, and a module that has the summary on its compile classpath but not
 * cassandra-all - which is exactly {@code :seastar-server} - then fails to resolve the factory.
 */
final class StatementSummaries {

	private StatementSummaries() {
		// hidden constructor
	}

	static CqlStatementSummary of(final Metadata metadata,
		final @Nullable CqlIdentifier sessionKeyspace, final String query) {
		final var coordinator = metadata.getNodes().values().stream().findFirst().orElse(null);

		return of(metadata, sessionKeyspace, CqlParsers.parse(coordinator, query));
	}

	private static CqlStatementSummary of(final Metadata metadata,
		final @Nullable CqlIdentifier sessionKeyspace, final CQLStatement.Raw raw) {
		if (raw instanceof UseStatement use) {
			return new KeyspaceSelected(use.keyspace());
		}
		if (raw instanceof CreateKeyspaceStatement.Raw create) {
			return keyspace(Change.CREATED, create.keyspaceName);
		}
		if (raw instanceof AlterKeyspaceStatement.Raw alter) {
			return keyspace(Change.UPDATED, FieldBindings.ALTER_KEYSPACE_NAME.require(alter));
		}
		if (raw instanceof DropKeyspaceStatement.Raw drop) {
			return keyspace(Change.DROPPED, FieldBindings.DROP_KEYSPACE_NAME.require(drop));
		}
		if (raw instanceof CreateTableStatement.Raw create) {
			return table(Change.CREATED, create.keyspace(), create.table(), sessionKeyspace);
		}
		if (raw instanceof AlterTableStatement.Raw alter) {
			return table(Change.UPDATED, FieldBindings.ALTER_TABLE_NAME.require(alter),
				sessionKeyspace);
		}
		if (raw instanceof DropTableStatement.Raw drop) {
			return table(Change.DROPPED, FieldBindings.DROP_TABLE_NAME.require(drop), sessionKeyspace);
		}
		if (raw instanceof CreateTypeStatement.Raw create) {
			return type(Change.CREATED, FieldBindings.CREATE_TYPE_NAME.require(create),
				sessionKeyspace);
		}
		if (raw instanceof AlterTypeStatement.Raw alter) {
			return type(Change.UPDATED, FieldBindings.ALTER_TYPE_NAME.require(alter), sessionKeyspace);
		}
		if (raw instanceof DropTypeStatement.Raw drop) {
			return type(Change.DROPPED, FieldBindings.DROP_TYPE_NAME.require(drop), sessionKeyspace);
		}
		// An index is reported as an update to the table it indexes, which is what a real node sends
		// and what a driver needs in order to refresh the right table.
		if (raw instanceof CreateIndexStatement.Raw create) {
			return table(Change.UPDATED, FieldBindings.CREATE_INDEX_TABLE_NAME.require(create),
				sessionKeyspace);
		}
		if (raw instanceof DropIndexStatement.Raw drop) {
			return droppedIndex(metadata, FieldBindings.DROP_INDEX_NAME.require(drop), sessionKeyspace);
		}

		return new Result();
	}

	private static CqlStatementSummary keyspace(final Change change, final String keyspace) {
		return new SchemaChanged(change, Target.KEYSPACE, keyspace, null);
	}

	private static CqlStatementSummary table(final Change change, final QualifiedName name,
		final @Nullable CqlIdentifier sessionKeyspace) {
		return table(change, name.hasKeyspace() ? name.getKeyspace() : null, name.getName(),
			sessionKeyspace);
	}

	private static CqlStatementSummary table(final Change change,
		final @Nullable String statementKeyspace, final String table,
		final @Nullable CqlIdentifier sessionKeyspace) {
		return new SchemaChanged(change, Target.TABLE, keyspaceOf(statementKeyspace, sessionKeyspace),
			table);
	}

	private static CqlStatementSummary type(final Change change, final UTName name,
		final @Nullable CqlIdentifier sessionKeyspace) {
		return new SchemaChanged(change, Target.TYPE, keyspaceOf(name.getKeyspace(), sessionKeyspace),
			name.getStringTypeName());
	}

	/**
	 * {@code DROP INDEX} names the index, never its table, so the table has to be found by looking
	 * through the keyspace - the same search {@code DropIndexHandler} does. A statement that names
	 * nothing findable is summarized against a null table: it is about to fail, or it is an
	 * {@code IF EXISTS} that will do nothing, and either way there is no table to refresh.
	 */
	private static CqlStatementSummary droppedIndex(final Metadata metadata,
		final QualifiedName name, final @Nullable CqlIdentifier sessionKeyspace) {
		final var keyspace = keyspaceOf(name.hasKeyspace() ? name.getKeyspace() : null,
			sessionKeyspace);
		final var index = CqlIdentifier.fromInternal(name.getName());
		final var table = metadata.getKeyspace(CqlIdentifier.fromInternal(keyspace))
			.stream()
			.flatMap(candidate -> candidate.getTables().values().stream())
			.filter(candidate -> candidate.getIndexes().containsKey(index))
			.findFirst()
			.map(TableMetadata::getName)
			.map(CqlIdentifier::asInternal)
			.orElse(null);

		return new SchemaChanged(Change.UPDATED, Target.TABLE, keyspace, table);
	}

	private static String keyspaceOf(final @Nullable String statementKeyspace,
		final @Nullable CqlIdentifier sessionKeyspace) {
		return Optional.ofNullable(statementKeyspace)
			.orElseGet(() -> sessionKeyspace == null ? "" : sessionKeyspace.asInternal());
	}

}
