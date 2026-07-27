/**
 * Internal. Not part of SeaStar's public API - the CQL statement pipeline (handlers, the two
 * registries, the translation layer) that {@code SeaStarCqlSession} builds and drives itself.
 *
 * <p>Every type here is public only because {@code SeaStarCqlSession} constructs it from the
 * {@code com.tagadvance.seastar} package and Java has no visibility modifier between
 * package-private and public that would let one package reach into another without opening it
 * up entirely. Nothing in this package is a supported entry point: it may change shape, be
 * renamed, or move without notice between releases. Build against {@code SeaStarCqlSession},
 * {@code SeaStarCqlSessionBuilder}, {@code SeaStarDriverContext} and the {@code SeaStar*} model
 * interfaces in {@code com.tagadvance.seastar} instead.
 */
package com.tagadvance.seastar.handlers;
