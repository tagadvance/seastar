/**
 * Small, dependency-free utility mixins shared across the storage model, independent of anything
 * CQL- or driver-specific. Currently just {@code SeaStarReadWriteLock}, the lock-delegation mixin
 * every {@code Volatile*} class implements - see the lock hierarchy in {@code AGENTS.md}.
 */
@NullMarked
package com.tagadvance.tools;

import org.jspecify.annotations.NullMarked;
