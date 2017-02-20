package org.chronos.chronodb.internal.impl.cache.util.lru;

import static com.google.common.base.Preconditions.*;

import java.util.function.Function;
import java.util.function.Supplier;

public interface UsageRegistry<T> {

	public void registerUsage(final T element);

	public int sizeInElements();

	public void clear();

	public void removeLeastRecentlyUsedElement();

	public Function<T, Object> getTopicResolutionFunction();

	public void addLeastRecentlyUsedRemoveListener(Object topic, RemoveListener<T> listener);

	public void removeLeastRecentlyUsedListener(Object topic, RemoveListener<T> listener);

	// =====================================================================================================================
	// DEFAULT METHODS
	// =====================================================================================================================

	public default void addLeastRecentlyUsedRemoveListenerToAnyTopic(final RemoveListener<T> listener) {
		checkNotNull(listener, "Precondition violation - argument 'listener' must not be NULL!");
		this.addLeastRecentlyUsedRemoveListener(null, listener);
	}

	public default void removeLeastRecentlyUsedElements(final int elementsToRemove) {
		for (int i = 0; i < elementsToRemove; i++) {
			this.removeLeastRecentlyUsedElement();
		}
	}

	public default void removeLeastRecentlyUsedUntil(final Supplier<Boolean> decision) {
		while (decision.get() == false) {
			this.removeLeastRecentlyUsedElement();
		}
	}

	public default boolean isEmpty() {
		return this.sizeInElements() <= 0;
	}

	public int getListenerCount();

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	public static interface RemoveListener<T> {

		public void objectRemoved(Object topic, T element);

	}

}
