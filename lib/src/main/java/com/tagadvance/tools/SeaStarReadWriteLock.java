package com.tagadvance.tools;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.UncheckedExecutionException;
import com.google.common.base.Throwables;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import org.jspecify.annotations.NonNull;

/**
 * Provides utility methods for working with {@link Lock locks}.
 */
@FunctionalInterface
public interface SeaStarReadWriteLock extends ReadWriteLock {

	/**
	 * The lock this object is guarded by.
	 *
	 * <p><strong>Must return the same instance on every call.</strong> The helpers below acquire and
	 * release through separate invocations, so an implementation that hands out a fresh lock each
	 * time would release something it never took. Every implementation returns a final field, or the
	 * field of the object it shares a lock with.
	 */
	ReadWriteLock lock();

	@Override
	@NonNull
	default Lock readLock() {
		return lock().readLock();
	}

	@Override
	@NonNull
	default Lock writeLock() {
		return lock().writeLock();
	}

	default void readLock(final Runnable runnable) {
		readLock().lock();
		try {
			runnable.run();
		} finally {
			readLock().unlock();
		}
	}

	default void writeLock(final Runnable runnable) {
		writeLock().lock();
		try {
			runnable.run();
		} finally {
			writeLock().unlock();
		}
	}

	default <V> V readLockUnchecked(final Callable<V> callable) {
		readLock().lock();
		try {
			return callable.call();
		} catch (final Exception e) {
			Throwables.throwIfUnchecked(e);

			throw new UncheckedExecutionException(e);
		} finally {
			readLock().unlock();
		}
	}

	default <V> V writeLockUnchecked(final Callable<V> callable) {
		writeLock().lock();
		try {
			return callable.call();
		} catch (final Exception e) {
			Throwables.throwIfUnchecked(e);

			throw new UncheckedExecutionException(e);
		} finally {
			writeLock().unlock();
		}
	}

}
