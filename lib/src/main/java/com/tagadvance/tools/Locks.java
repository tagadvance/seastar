package com.tagadvance.tools;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.UncheckedExecutionException;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.jspecify.annotations.NonNull;

/**
 * Provides utility methods for working with {@link Lock locks}.
 */
public final class Locks {

	private Locks() {
		// prevent instantiation
	}

	public static SeaStarReadWriteLock newLock() {
		return new SeaStarReadWriteLockImpl();
	}

	private static class SeaStarReadWriteLockImpl implements SeaStarReadWriteLock {

		private final ReadWriteLock lock = new ReentrantReadWriteLock();

		@Override
		@NonNull
		public Lock readLock() {
			return lock.readLock();
		}

		@Override
		@NonNull
		public Lock writeLock() {
			return lock.writeLock();
		}

	}

	public interface SeaStarReadWriteLock extends ReadWriteLock {

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

		default <V> V readLock(final Callable<V> callable) throws Exception {
			readLock().lock();
			try {
				return callable.call();
			} finally {
				readLock().unlock();
			}
		}

		default <V> V writeLock(final Callable<V> callable) throws Exception {
			writeLock().lock();
			try {
				return callable.call();
			} finally {
				writeLock().unlock();
			}
		}

		default <V> V readLockUnchecked(final Callable<V> callable)
			throws UncheckedExecutionException {
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

}
