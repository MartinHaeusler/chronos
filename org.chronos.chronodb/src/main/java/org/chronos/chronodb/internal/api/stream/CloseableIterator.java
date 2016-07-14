package org.chronos.chronodb.internal.api.stream;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

import org.chronos.chronodb.internal.impl.stream.ConcatenatedCloseableIterator;
import org.chronos.chronodb.internal.impl.stream.TransformingCloseableIterator;

/**
 * A {@link CloseableIterator} is simply a regular {@link Iterator} that has to be {@link #close() closed} explicitly.
 *
 * <p>
 * This class does <b>not</b> extend {@link Iterator} for a very good reason. Not implementing {@link Iterator} prevents
 * assignment of an {@link CloseableIterator} instance to a {@link Iterator} variable. This would be dangerous, because
 * the {@link Iterator}-typed variable may be passed on as a regular iterator to other code, and {@link #close()} would
 * never be invoked.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @param <T>
 *            The element type which is returned by {@link #next()}.
 */
public interface CloseableIterator<T> extends AutoCloseable {

	// =====================================================================================================================
	// STATIC HELPERS
	// =====================================================================================================================

	/**
	 * Concatenates the two given iterators into one common iterator.
	 *
	 * <p>
	 * Calling {@link #close()} on the resulting iterator will close <b>all</b> of the "child" iterators. The iteration
	 * process itself is lazy: the first iterator will be fully consumed (and closed) before the second iterator is
	 * touched by {@link #next()}.
	 *
	 *
	 * @param iterators
	 *            The iterators to concatenate. Must not be <code>null</code>.
	 * @return The combined concatenated iterator. Never <code>null</code>.
	 */
	public static <E> CloseableIterator<E> concat(final Iterator<CloseableIterator<E>> iterators) {
		checkNotNull(iterators, "Precondition violation - argument 'iterators' must not be NULL!");
		return new ConcatenatedCloseableIterator<E>(iterators);
	}

	/**
	 * Transforms every element in the given iterator by applying the given function on it, resulting in a new iterator.
	 *
	 * <p>
	 * The given function will be applied in a lazy way, i.e. values will be converted on the fly when they are
	 * requested via {@link #next()}.
	 *
	 * <p>
	 * Closing the resulting iterator will also close the wrapped iterator.
	 *
	 * @param iterator
	 *            The iterator to transform. Must not be <code>null</code>.
	 * @param function
	 *            The transformation function to apply to each element of the iterator. Must not be <code>null</code>.
	 * @return The transformed iterator. Never <code>null</code>.
	 */
	public static <E, F> CloseableIterator<F> transform(final CloseableIterator<E> iterator,
			final Function<? super E, ? extends F> function) {
		checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
		checkNotNull(function, "Precondition violation - argument 'function' must not be NULL!");
		return new TransformingCloseableIterator<>(iterator, function);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	/**
	 * Same as the regular {@link Iterator#next()}.
	 *
	 * @return The next element in this iterator. Depending on the implementation, this may be <code>null</code>.
	 */
	public T next();

	/**
	 * Same as the regular {@link Iterator#hasNext()}.
	 *
	 * @return <code>true</code> if there is another element to be returned in {@link #next()}, otherwise
	 *         <code>false</code>.
	 */
	public boolean hasNext();

	/**
	 * Closes this iterator.
	 *
	 * <p>
	 * Any further call to {@link #hasNext()} will return <code>false</code>, and any further call to {@link #next()}
	 * will fail with an exception.
	 *
	 * <p>
	 * Closing a closeable iterator more than once is permitted, but has no effect.
	 */
	@Override
	public void close();

	/**
	 * Checks if this iterator has been closed.
	 *
	 * @return <code>true</code> if this iterator has already been closed, otherwise <code>false</code>.
	 */
	public boolean isClosed();

	// =====================================================================================================================
	// EXTENSION METHODS
	// =====================================================================================================================

	/**
	 * Performs the given action for each remaining element in this iterator.
	 *
	 * <p>
	 * <b>/!\ This will NOT close this iterator!</b> The iterator will still need to be {@linkplain #close() closed}
	 * manually afterwards.
	 *
	 * @param action
	 *            The action to perform for each remaining element. Must not be <code>null</code>.
	 */
	public default void forEachRemaining(final Consumer<T> action) {
		while (this.hasNext()) {
			action.accept(this.next());
		}
	}

}
