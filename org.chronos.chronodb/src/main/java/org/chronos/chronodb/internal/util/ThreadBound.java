package org.chronos.chronodb.internal.util;

import static com.google.common.base.Preconditions.*;

import java.lang.ref.WeakReference;
import java.util.Map;

import com.google.common.collect.MapMaker;

/**
 * A {@link ThreadBound} object is similar to a {@link ThreadLocal}, except that the memory management is different.
 *
 * <p>
 * In a regular {@link ThreadLocal}, the value is stored in the thread until the thread dies. This is true even if there is no way to access this value in regular Java code anymore. In a {@link ThreadBound} object, the value is cleared if:
 *
 * <ul>
 * <li>The thread dies and is GC'ed <b>OR</b>
 * <li>the object owning the {@link ThreadBound} is no longer reachable and GC'ed.
 * </ul>
 *
 * In contrast to {@link ThreadLocal}, this class also allows to clear the contained value for <b>all</b> threads via {@link #clearValueForAllThreads()}.
 *
 * @author martin.haeusler@uibk.ac.at
 *
 * @param <T>
 *            The type of element contained in this object.
 */
public class ThreadBound<T> {

	// =================================================================================================================
	// STATIC FACTORY METHODS
	// =================================================================================================================

	/**
	 * Creates a new {@link ThreadBound} instance.
	 *
	 * @return The newly created instance. Never <code>null</code>.
	 */
	public static <T> ThreadBound<T> create() {
		MapMaker mapMaker = new MapMaker();
		Map<Thread, T> map = mapMaker.weakKeys().makeMap();
		return new ThreadBound<>(map);
	}

	/**
	 * Creates a new {@link ThreadBound} instance with {@linkplain WeakReference weakly referenced} values.
	 *
	 * @return The newly created instance. Never <code>null</code>.
	 */
	public static <T> ThreadBound<T> createWeakReference() {
		MapMaker mapMaker = new MapMaker();
		Map<Thread, T> map = mapMaker.weakKeys().weakValues().makeMap();
		return new ThreadBound<>(map);
	}

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	/** This is the internal mapping from thread to bound value. Note that it is a weak hash map, i.e. GC'ed threads will be removed from the map. */
	private final Map<Thread, T> map;

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	/**
	 * Creates a new {@link ThreadBound} instance.
	 *
	 * @param map
	 *            The map to use internally. Must not be <code>null</code>.
	 */
	private ThreadBound(final Map<Thread, T> map) {
		// private on purpose; use the static factory methods.
		checkNotNull(map, "Precondition violation - argument 'map' must not be NULL!");
		this.map = map;
	}

	/**
	 * Returns the value associated with the current thread.
	 *
	 * @return The value. May be <code>null</code> if the thread does not have a value yet (or was explicitly assigned a <code>null</code> value).
	 */
	public synchronized T get() {
		return this.map.get(Thread.currentThread());
	}

	/**
	 * Associates the given value with the current thread.
	 *
	 * @param value
	 *            The value to store. May be <code>null</code>.
	 *
	 * @return The previous value. May be <code>null</code>. If no previous value existed for the given thread, <code>null</code> will be returned.
	 */
	public synchronized T set(final T value) {
		if (value == null) {
			return this.unset();
		}
		return this.map.put(Thread.currentThread(), value);
	}

	/**
	 * Removes the value associated with the current thread.
	 *
	 * <p>
	 * After invoking this method, calls to {@link #get()} will always return <code>null</code> until a new, non-<code>null</code> value is assigned via {@link #set(Object)}.
	 *
	 * @return The previously assigned value. May be <code>null</code>. If no previous value existed for the given thread, <code>null</code> will be returned.
	 */
	public synchronized T unset() {
		return this.map.remove(Thread.currentThread());
	}

	/**
	 * Clears the stored value for all threads.
	 *
	 * <p>
	 * After invoking this method, calls to {@link #get()} will always return <code>null</code> for all threads until a new, non-<code>null</code> value is assigned via {@link #set(Object)}.
	 *
	 */
	public synchronized void clearValueForAllThreads() {
		this.map.clear();
	}

}
