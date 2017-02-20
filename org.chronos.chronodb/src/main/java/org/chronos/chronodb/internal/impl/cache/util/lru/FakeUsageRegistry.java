package org.chronos.chronodb.internal.impl.cache.util.lru;

import java.util.function.Function;

public class FakeUsageRegistry<T> implements UsageRegistry<T> {

	// =====================================================================================================================
	// SINGLETON PATTERN
	// =====================================================================================================================

	private static final FakeUsageRegistry<?> INSTANCE;

	static {
		INSTANCE = new FakeUsageRegistry<Object>();
	}

	@SuppressWarnings("unchecked")
	public static <T> FakeUsageRegistry<T> getInstance() {
		return (FakeUsageRegistry<T>) INSTANCE;
	}

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	private FakeUsageRegistry() {
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public void registerUsage(final T element) {
		// do nothing
	}

	@Override
	public int sizeInElements() {
		// we are always empty
		return 0;
	}

	@Override
	public void clear() {
		// do nothing
	}

	@Override
	public void removeLeastRecentlyUsedElement() {
		// do nothing
	}

	@Override
	public void removeLeastRecentlyUsedElements(final int elementsToRemove) {
		// do nothing
	}

	@Override
	public void addLeastRecentlyUsedRemoveListener(final Object topic, final RemoveListener<T> handler) {
		// do nothing
	}

	@Override
	public void removeLeastRecentlyUsedListener(final Object topic, final org.chronos.chronodb.internal.impl.cache.util.lru.UsageRegistry.RemoveListener<T> listener) {
		// do nothing
	}

	@Override
	public int getListenerCount() {
		return 0;
	}

	@Override
	public Function<T, Object> getTopicResolutionFunction() {
		return (element) -> null;
	}

}
