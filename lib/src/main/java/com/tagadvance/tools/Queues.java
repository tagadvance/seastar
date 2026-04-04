package com.tagadvance.tools;

import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import org.jspecify.annotations.NonNull;

/**
 * Provides utility methods for working with {@link Queue queues}.
 */
public final class Queues {

	private Queues() {
		// hidden constructor
	}

	/**
	 * Returns an unmodifiable view of the specified queue. Query operations on the returned queue
	 * "read through" to the specified queue, and attempts to modify the returned queue, whether
	 * direct or via its iterator, result in an {@link UnsupportedOperationException}.
	 *
	 * @param queue the {@link Queue queue} for which an unmodifiable view is to be returned
	 * @param <E>   the class of the objects in the {@link Queue queue}
	 * @return an unmodifiable view of the specified {@link Queue queue}
	 */
	public static <E> Queue<E> unmodifiableQueue(final @Nonnull Queue<E> queue) {
		return queue instanceof UnmodifiableQueue ? queue : new UnmodifiableQueue<>(queue);
	}

	private static class UnmodifiableQueue<E> implements Queue<E> {

		private final Queue<E> queue;

		public UnmodifiableQueue(final @Nonnull Queue<E> queue) {
			this.queue = queue;
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		public boolean contains(final Object o) {
			return queue.contains(o);
		}

		@Override
		@Nonnull
		public Iterator<E> iterator() {
			final Iterator<E> iterator = queue.iterator();

			return new Iterator<>() {

				@Override
				public boolean hasNext() {
					return iterator.hasNext();
				}

				@Override
				public E next() {
					return iterator.next();
				}

				@Override
				public void remove() {
					throw newImmutableException();
				}

			};
		}

		@Override
		@Nonnull
		public Object[] toArray() {
			return queue.toArray();
		}

		@Override
		@Nonnull
		public <T> T[] toArray(final @Nonnull T[] a) {
			return queue.toArray(a);
		}

		@Override
		public boolean add(final E e) {
			throw newImmutableException();
		}

		@Override
		public boolean remove(final Object o) {
			throw newImmutableException();
		}

		@Override
		public boolean containsAll(final @Nonnull Collection<?> c) {
			return queue.containsAll(c);
		}

		@Override
		public boolean addAll(final @NonNull Collection<? extends E> c) {
			throw newImmutableException();
		}

		@Override
		public boolean removeAll(final @NonNull Collection<?> c) {
			throw newImmutableException();
		}

		@Override
		public boolean retainAll(final @NonNull Collection<?> c) {
			throw newImmutableException();
		}

		@Override
		public void clear() {
			throw newImmutableException();
		}

		@Override
		public boolean offer(final E e) {
			throw newImmutableException();
		}

		@Override
		public E remove() {
			throw newImmutableException();
		}

		@Override
		public E poll() {
			throw newImmutableException();
		}

		@Override
		public E element() {
			return queue.element();
		}

		@Override
		public E peek() {
			return queue.peek();
		}

		private static RuntimeException newImmutableException() {
			return new UnsupportedOperationException("This queue is immutable");
		}

	}

}
