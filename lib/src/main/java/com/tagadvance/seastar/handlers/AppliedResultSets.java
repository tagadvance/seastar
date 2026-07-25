package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.tagadvance.seastar.SeaStarAsyncResultSet;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarTable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.jspecify.annotations.NonNull;

/**
 * Builds the one-row {@code [applied]} result set that lightweight transactions (IF / IF NOT
 * EXISTS / IF EXISTS) return. The first column is a boolean {@code [applied]}; on a failed
 * condition the current values of the conflicting row follow.
 */
final class AppliedResultSets {

	private static final CqlIdentifier APPLIED = CqlIdentifier.fromInternal("[applied]");

	private AppliedResultSets() {
	}

	static AsyncResultSet of(final SeaStarDriverContext context, final SeaStarTable table,
		final ExecutionInfo executionInfo, final boolean applied) {
		return build(context, table, executionInfo, applied, null);
	}

	static AsyncResultSet ofExisting(final SeaStarDriverContext context, final SeaStarTable table,
		final ExecutionInfo executionInfo, final Row existing) {
		return build(context, table, executionInfo, false, existing);
	}

	private static AsyncResultSet build(final SeaStarDriverContext context, final SeaStarTable table,
		final ExecutionInfo executionInfo, final boolean applied, final Row existing) {
		final var codecRegistry = context.getCodecRegistry();
		final var protocolVersion = context.getProtocolVersion();

		final List<ColumnDefinition> definitions = new ArrayList<>();
		definitions.add(appliedColumn(table));
		if (existing != null) {
			final var existingDefinitions = existing.getColumnDefinitions();
			for (int i = 0; i < existingDefinitions.size(); i++) {
				definitions.add(existingDefinitions.get(i));
			}
		}
		final var columnDefinitions = DefaultColumnDefinitions.valueOf(definitions);

		final var row = new AppliedRow(columnDefinitions, codecRegistry, protocolVersion, applied,
			existing);
		final var data = new LinkedList<Row>();
		data.add(row);

		return new SeaStarAsyncResultSet(columnDefinitions, executionInfo, data);
	}

	private static ColumnDefinition appliedColumn(final SeaStarTable table) {
		return new ColumnDefinition() {

			@Override
			@NonNull
			public CqlIdentifier getKeyspace() {
				return table.getKeyspace();
			}

			@Override
			@NonNull
			public CqlIdentifier getTable() {
				return table.getName();
			}

			@Override
			@NonNull
			public CqlIdentifier getName() {
				return APPLIED;
			}

			@Override
			@NonNull
			public DataType getType() {
				return DataTypes.BOOLEAN;
			}

			@Override
			public boolean isDetached() {
				return false;
			}

			@Override
			public void attach(final @NonNull AttachmentPoint attachmentPoint) {
			}

		};
	}

	private record AppliedRow(ColumnDefinitions definitions, CodecRegistry codecRegistry,
		ProtocolVersion protocolVersion, boolean applied, Row existing) implements Row {

		@Override
		public boolean isDetached() {
			return false;
		}

		@Override
		public void attach(final @NonNull AttachmentPoint attachmentPoint) {
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public CodecRegistry codecRegistry() {
			return codecRegistry;
		}

		@Override
		@NonNull
		public ProtocolVersion protocolVersion() {
			return protocolVersion;
		}

		@Override
		public int size() {
			return definitions.size();
		}

		@Override
		@NonNull
		public DataType getType(final int i) {
			return definitions.get(i).getType();
		}

		@Override
		public ByteBuffer getBytesUnsafe(final int i) {
			if (i == 0) {
				return codecRegistry.codecFor(DataTypes.BOOLEAN, Boolean.class)
					.encode(applied, protocolVersion);
			}

			return existing.getBytesUnsafe(i - 1);
		}

		@Override
		public int firstIndexOf(final @NonNull String name) {
			return definitions.firstIndexOf(name);
		}

		@Override
		@NonNull
		public DataType getType(final @NonNull String name) {
			return definitions.get(name).getType();
		}

		@Override
		public int firstIndexOf(final @NonNull CqlIdentifier id) {
			return definitions.firstIndexOf(id);
		}

		@Override
		@NonNull
		public DataType getType(final @NonNull CqlIdentifier id) {
			return definitions.get(id).getType();
		}

		@Override
		@NonNull
		public ColumnDefinitions getColumnDefinitions() {
			return definitions;
		}

	}

}
