/**
 * SeaStar: an in-memory implementation of the DataStax Java driver's {@code CqlSession}, intended
 * as a fast, in-process alternative to TestContainers for tests.
 *
 * <p>The supported entry points are {@code SeaStarCqlSession} (via {@code .builder()...build()}),
 * {@code SeaStarCqlSessionBuilder}, {@code SeaStarDriverContext} and the {@code SeaStar*} model
 * interfaces ({@code SeaStarKeyspace}, {@code SeaStarTable}, {@code SeaStarColumn},
 * {@code SeaStarRow}, {@code SeaStarUserDefinedType}, {@code SeaStarUdtValue}) for populating a
 * session directly in a test. Everything else in this package is an implementation detail of the
 * request pipeline and storage model - see {@code AGENTS.md} for the architecture - and is not a
 * supported entry point even where the compiler cannot enforce that.
 */
@NullMarked
package com.tagadvance.seastar;

import org.jspecify.annotations.NullMarked;
