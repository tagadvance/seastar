package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.UserTypes;

/**
 * Resolves a parsed term into the Java value it stands for, against the type of the column or field
 * it is assigned to or compared with. Shared by every handler that reads values out of a statement,
 * so that a term means the same thing in an INSERT, an UPDATE, a WHERE clause, and an IF condition.
 */
final class Terms {

	private Terms() {
	}

	/**
	 * A UDT literal can nest further literals and bind markers, so terms resolve recursively against
	 * the type of the field they are being assigned to rather than only at the top level.
	 */
	static Object resolve(final Object term, final DataType dataType,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		if (term instanceof AbstractMarker.Raw marker) {
			final var bindIndex = Reflections.getDeclaredField(marker, "bindIndex", Integer.class)
				.orElseThrow();

			return bindIndex < bindings.length ? bindings[bindIndex] : null;
		} else if (term instanceof UserTypes.Literal literal) {
			return toUdtValue(literal, dataType, codecRegistry, coordinator, bindings);
		} else if (term instanceof Constants.Literal literal) {
			return codecRegistry.codecFor(dataType).parse(literal.getText());
		}

		throw new UnsupportedOperationException("Unsupported term %s".formatted(term));
	}

	private static UdtValue toUdtValue(final UserTypes.Literal literal, final DataType dataType,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		if (!(dataType instanceof UserDefinedType udt)) {
			throw new InvalidQueryException(coordinator,
				"Invalid user type literal for a column of type %s".formatted(dataType.asCql(true, true)));
		}

		// Fields absent from the literal keep the null they were initialised with, matching
		// UserTypes.Literal.prepare, which substitutes NULL_LITERAL for anything not named.
		final var value = udt.newValue();
		literal.entries.forEach((field, fieldTerm) -> {
			final var name = CqlIdentifier.fromInternal(field.toString());
			final var index = udt.firstIndexOf(name);
			if (index < 0) {
				throw new InvalidQueryException(coordinator,
					"Unknown field '%s' in value of user defined type %s".formatted(name.asInternal(),
						udt.getName().asInternal()));
			}
			final var fieldType = udt.getFieldTypes().get(index);
			final TypeCodec<Object> codec = codecRegistry.codecFor(fieldType);
			value.set(index, resolve(fieldTerm, fieldType, codecRegistry, coordinator, bindings), codec);
		});

		return value;
	}

}
