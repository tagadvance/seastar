package com.tagadvance.tools;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.UncheckedExecutionException;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import org.jspecify.annotations.NonNull;

/**
 * Provides utility methods for working with {@link Lock locks}.
 */
@FunctionalInterface
public interface SeaStarReadWriteLock extends ReadWriteLock {

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

	default <E extends Exception> void readLock(final Runnable runnable) {
		readLock().lock();
		try {
			runnable.run();
		} finally {
			readLock().unlock();
		}
	}

	default <E extends Exception> void writeLock(final Runnable runnable) {
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
			if (e instanceof RuntimeException re) {
				throw re;
			}

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
			if (e instanceof RuntimeException re) {
				throw re;
			}

			throw new UncheckedExecutionException(e);
		} finally {
			writeLock().unlock();
		}
	}

}
