package com.tagadvance.seastar.handlers;

import org.apache.cassandra.cql3.Operator;

/**
 * The comparison an operator in a WHERE clause or an IF condition stands for.
 *
 * <p>Handlers compare against these rather than against {@code org.apache.cassandra.cql3.Operator},
 * so the parse tree stops at the translation layer. Every operator the grammar accepts is mapped,
 * including the ones SeaStar cannot evaluate yet: an unimplemented operator is rejected where a
 * restriction becomes a predicate, in one place instead of once each in SELECT, UPDATE and DELETE.
 *
 * <p>The symbols are Cassandra's own, so a message naming an operator reads the same as it did when
 * it was formatted straight from the parse tree.
 */
enum CqlOperator {

	EQ("="),
	NEQ("!="),
	LT("<"),
	LTE("<="),
	GT(">"),
	GTE(">="),
	IN("IN"),
	CONTAINS("CONTAINS"),
	CONTAINS_KEY("CONTAINS KEY"),
	LIKE("LIKE"),
	IS_NOT("IS NOT"),
	ANN("ANN OF");

	private final String symbol;

	CqlOperator(final String symbol) {
		this.symbol = symbol;
	}

	/**
	 * The switch is exhaustive on purpose: an operator added by a cassandra-all upgrade fails the
	 * build here rather than being silently mistranslated.
	 */
	static CqlOperator of(final Operator operator) {
		return switch (operator) {
			case EQ -> EQ;
			case NEQ -> NEQ;
			case LT -> LT;
			case LTE -> LTE;
			case GT -> GT;
			case GTE -> GTE;
			case IN -> IN;
			case CONTAINS -> CONTAINS;
			case CONTAINS_KEY -> CONTAINS_KEY;
			case IS_NOT -> IS_NOT;
			// The parser splits LIKE by where the wildcard sits; the distinction is the pattern's,
			// not the operator's.
			case LIKE, LIKE_PREFIX, LIKE_SUFFIX, LIKE_CONTAINS, LIKE_MATCHES -> LIKE;
			case ANN -> ANN;
		};
	}

	@Override
	public String toString() {
		return symbol;
	}

}
