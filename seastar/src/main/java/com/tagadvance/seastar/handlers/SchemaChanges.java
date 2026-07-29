package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TableChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TypeChangeEvent;
import com.tagadvance.seastar.SeaStarDriverContext;

/**
 * Announces a schema change on the driver's event bus, which is what evicts the prepared statements
 * whose bind or result variables the change invalidates. {@code SeaStarCqlPrepareAsyncProcessor}
 * registers the listeners; without an announcement a statement prepared before the change keeps
 * answering with the old columns.
 *
 * <p>Both events name an old and a new object. SeaStar mutates its storage in place rather than
 * swapping an immutable snapshot the way the real driver's metadata does, so there is no "before"
 * to hand over and the same instance is passed twice. The listeners only ever read one of the two.
 */
final class SchemaChanges {

	private SchemaChanges() {
		// hidden constructor
	}

	static void tableChanged(final SeaStarDriverContext context, final TableMetadata table) {
		eventBus(context).fire(TableChangeEvent.updated(table, table));
	}

	static void typeChanged(final SeaStarDriverContext context, final UserDefinedType type) {
		eventBus(context).fire(TypeChangeEvent.updated(type, type));
	}

	static void typeDropped(final SeaStarDriverContext context, final UserDefinedType type) {
		eventBus(context).fire(TypeChangeEvent.dropped(type));
	}

	/**
	 * {@code VolatileDriverContext} extends the driver's own {@code DefaultDriverContext}, so it
	 * carries the driver's event bus; the cast is the same one the prepare processor makes to
	 * subscribe to it.
	 */
	private static EventBus eventBus(final SeaStarDriverContext context) {
		return ((InternalDriverContext) context).getEventBus();
	}

}
