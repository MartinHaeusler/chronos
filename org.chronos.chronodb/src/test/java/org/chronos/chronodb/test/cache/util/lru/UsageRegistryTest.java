package org.chronos.chronodb.test.cache.util.lru;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.RangedGetResult;
import org.chronos.chronodb.internal.impl.cache.util.lru.DefaultUsageRegistry;
import org.chronos.chronodb.internal.impl.cache.util.lru.RangedGetResultUsageRegistry;
import org.chronos.chronodb.internal.impl.cache.util.lru.UsageRegistry;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@Category(UnitTest.class)
public class UsageRegistryTest extends ChronoDBUnitTest {

	@Test
	public void canCreateDefaultUsageRegistry() {
		UsageRegistry<Integer> registry = new DefaultUsageRegistry<Integer>((e) -> e);
		assertNotNull(registry);
	}

	@Test
	public void registeringUsagesWorks() {
		UsageRegistry<Integer> registry = new DefaultUsageRegistry<Integer>((e) -> e);
		assertNotNull(registry);
		registry.registerUsage(1);
		registry.registerUsage(2);
		registry.registerUsage(3);
		registry.registerUsage(4);
		registry.registerUsage(3);
		registry.registerUsage(2);
		registry.registerUsage(1);
		assertEquals(4, registry.size());
	}

	@Test
	public void removingLeastRecentlyUsedElementWorks() {
		UsageRegistry<Integer> registry = new DefaultUsageRegistry<Integer>((e) -> e);
		assertNotNull(registry);
		// do some registration stuff
		registry.registerUsage(1);
		this.sleep(1);
		registry.registerUsage(2);
		this.sleep(1);
		registry.registerUsage(3);
		this.sleep(1);
		registry.registerUsage(4);
		this.sleep(1);
		registry.registerUsage(3);
		this.sleep(1);
		registry.registerUsage(2);
		this.sleep(1);
		registry.registerUsage(1);
		// create the evict listener and attach it
		AssertElementsRemovedListener<Integer> listener = new AssertElementsRemovedListener<>(4, 3, 2, 1);
		registry.addLeastRecentlyUsedRemoveListenerToAnyTopic(listener);
		// clear the registry by removing the least recently used element, one at a time
		while (registry.isEmpty() == false) {
			registry.removeLeastRecentlyUsedElement();
		}
		// assert that the elements have been removed in the correct order
		listener.assertAllExpectedElementsRemoved();
	}

	@Test
	public void removingLeastRecentlyUsedElementsUntilSizeIsReachedWorks() {
		UsageRegistry<Integer> registry = new DefaultUsageRegistry<Integer>((e) -> e);
		assertNotNull(registry);
		// do some registration stuff
		registry.registerUsage(1);
		this.sleep(1);
		registry.registerUsage(2);
		this.sleep(1);
		registry.registerUsage(3);
		this.sleep(1);
		registry.registerUsage(4);
		this.sleep(1);
		registry.registerUsage(3);
		this.sleep(1);
		registry.registerUsage(2);
		this.sleep(1);
		registry.registerUsage(1);
		// create the evict listener and attach it
		AssertElementsRemovedListener<Integer> listener = new AssertElementsRemovedListener<>(4, 3);
		registry.addLeastRecentlyUsedRemoveListenerToAnyTopic(listener);
		// clear the registry by removing the least recently used element, one at a time
		registry.removeLeastRecentlyUsedUntilSizeIs(2);
		// assert that the elements have been removed in the correct order
		listener.assertAllExpectedElementsRemoved();
	}

	@Test
	public void removingNonExistingElementsDoesntCrash() {
		UsageRegistry<Integer> registry = new DefaultUsageRegistry<Integer>((e) -> e);
		assertNotNull(registry);
		registry.removeLeastRecentlyUsedElement();
		assertEquals(0, registry.size());
	}

	@Test
	public void topicBasedListenersOnlyReceiveNotificationsOnTheirTopic() {
		UsageRegistry<RangedGetResult<?>> registry = new RangedGetResultUsageRegistry();
		assertNotNull(registry);

		// prepare some keys (= topics)
		QualifiedKey qKey1 = QualifiedKey.createInDefaultKeyspace("Test1");
		QualifiedKey qKey2 = QualifiedKey.createInDefaultKeyspace("Test2");
		QualifiedKey qKey3 = QualifiedKey.createInDefaultKeyspace("Test3");

		// prepare some values
		RangedGetResult<?> rgr1 = RangedGetResult.create(qKey1, "Hello", Period.createRange(100, 200));
		RangedGetResult<?> rgr2 = RangedGetResult.create(qKey2, "World", Period.createRange(100, 200));
		RangedGetResult<?> rgr3 = RangedGetResult.create(qKey3, "Foo", Period.createRange(100, 200));

		// add the values to the registry (most recently used = rgr1, least recently used = rgr3)
		registry.registerUsage(rgr3);
		registry.registerUsage(rgr2);
		registry.registerUsage(rgr1);

		// add the remove listeners
		AssertElementsRemovedListener<RangedGetResult<?>> globalListener = new AssertElementsRemovedListener<>(rgr3,
				rgr2);
		AssertElementsRemovedListener<RangedGetResult<?>> qKey2Listener = new AssertElementsRemovedListener<>(rgr2);
		AssertElementsRemovedListener<RangedGetResult<?>> qKey1Listener = new AssertElementsRemovedListener<>();
		registry.addLeastRecentlyUsedRemoveListenerToAnyTopic(globalListener);
		registry.addLeastRecentlyUsedRemoveListener(qKey2, qKey2Listener);
		registry.addLeastRecentlyUsedRemoveListener(qKey1, qKey1Listener);

		registry.removeLeastRecentlyUsedUntilSizeIs(1);

		// now, our global listener should have received both remove calls
		globalListener.assertAllExpectedElementsRemoved();
		// in contrast, our "qKey2"-topic listener should have received only one call
		qKey2Listener.assertAllExpectedElementsRemoved();
		// ... and finally, our "qKey1"-topic listener should have received no calls at all
		qKey1Listener.assertAllExpectedElementsRemoved();
	}

	@Test
	public void globalListenersReceiveNotificationsOnAllTopics() {

	}

	@Test
	public void concurrentAccessWorks() {
		UsageRegistry<Integer> registry = new DefaultUsageRegistry<Integer>((e) -> e);
		assertNotNull(registry);

		ElementRemoveCountListener<Integer> listener = new ElementRemoveCountListener<>();
		registry.addLeastRecentlyUsedRemoveListenerToAnyTopic(listener);

		int registrationRepeats = 100;
		Runnable registrationWorker = () -> {
			int round = 0;
			while (round < registrationRepeats) {
				int value = round % 100;
				registry.registerUsage(value);
				long wait = Math.round(Math.random() * 5);
				this.sleep(wait);
				round++;
			}
		};
		Runnable lruRemovalWorker = () -> {
			while (true) {
				registry.removeLeastRecentlyUsedUntilSizeIs(20);
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					return;
				}
			}
		};
		int registrationWorkerThreadCount = 30;
		Thread lruRemovalWorkerThread = new Thread(lruRemovalWorker);
		lruRemovalWorkerThread.start();
		Set<Thread> registrationWorkerThreads = Sets.newHashSet();
		for (int i = 0; i < registrationWorkerThreadCount; i++) {
			Thread thread = new Thread(registrationWorker);
			registrationWorkerThreads.add(thread);
			thread.start();
		}
		// wait until the add workers are done
		for (Thread worker : registrationWorkerThreads) {
			try {
				worker.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// stop the cleanup worker
		lruRemovalWorkerThread.interrupt();
		try {
			lruRemovalWorkerThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// assert that the registry has at most 20 elements
		assertTrue(20 >= registry.size());
		// assert that removals indeed have happened
		assertTrue(listener.getRemoveCallCount() > 0);
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	private static class AssertElementsRemovedListener<T> implements UsageRegistry.RemoveListener<T> {

		private List<T> elementsToBeRemoved;

		@SafeVarargs
		public AssertElementsRemovedListener(final T... elementsToBeRemoved) {
			this.elementsToBeRemoved = Lists.newArrayList(elementsToBeRemoved);
		}

		@Override
		public void objectRemoved(final Object topic, final T element) {
			if (this.elementsToBeRemoved.isEmpty()) {
				fail("No further elements should have been removed, but element '" + element + "' was removed!");
			}
			Object elementToBeRemoved = this.elementsToBeRemoved.get(0);
			if (elementToBeRemoved.equals(element) == false) {
				fail("Element '" + elementToBeRemoved + "' should have been removed next, but '" + element
						+ "' was removed instead!");
			}
			this.elementsToBeRemoved.remove(0);
		}

		public boolean areAllExpectedElementsRemoved() {
			return this.elementsToBeRemoved.isEmpty();
		}

		public void assertAllExpectedElementsRemoved() {
			assertTrue("All elements should have been removed, but '" + this.elementsToBeRemoved
					+ "' have not yet been removed!", this.areAllExpectedElementsRemoved());
		}

	}

	private static class ElementRemoveCountListener<T> implements UsageRegistry.RemoveListener<T> {

		private AtomicInteger removeCalls = new AtomicInteger(0);

		@Override
		public void objectRemoved(final Object topic, final T element) {
			this.removeCalls.incrementAndGet();
		}

		public int getRemoveCallCount() {
			return this.removeCalls.get();
		}

	}

}
