package org.chronos.chronodb.test.cache.mosaic;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Set;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.cache.CacheGetResult;
import org.chronos.chronodb.internal.impl.cache.CacheStatisticsImpl;
import org.chronos.chronodb.internal.impl.cache.mosaic.MosaicRow;
import org.chronos.chronodb.internal.impl.cache.util.lru.FakeUsageRegistry;
import org.chronos.chronodb.internal.impl.cache.util.lru.RangedGetResultUsageRegistry;
import org.chronos.chronodb.internal.impl.cache.util.lru.UsageRegistry;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class MosaicRowTest extends ChronoDBUnitTest {

	@Test
	public void canCreateMosaicRow() {
		UsageRegistry<GetResult<?>> lruRegistry = FakeUsageRegistry.getInstance();
		QualifiedKey rowKey = QualifiedKey.createInDefaultKeyspace("Test");
		MosaicRow row = new MosaicRow(rowKey, lruRegistry, new CacheStatisticsImpl(), this::noSizeChangeHandler);
		assertNotNull(row);
	}

	@Test
	public void putAndGetAreConsistent() {
		UsageRegistry<GetResult<?>> lruRegistry = FakeUsageRegistry.getInstance();
		QualifiedKey rowKey = QualifiedKey.createInDefaultKeyspace("Test");
		MosaicRow row = new MosaicRow(rowKey, lruRegistry, new CacheStatisticsImpl(), this::noSizeChangeHandler);

		GetResult<Object> simulatedGetResult = GetResult.create(rowKey, "Hello", Period.createRange(100, 200));
		row.put(simulatedGetResult);
		assertEquals(1, row.size());

		{// get in between the ranges
			CacheGetResult<Object> cacheGet = row.get(150);
			assertNotNull(cacheGet);
			assertTrue(cacheGet.isHit());
			assertFalse(cacheGet.isMiss());
			assertEquals("Hello", cacheGet.getValue());
		}

		{ // get at the lower boundary
			CacheGetResult<Object> cacheGet = row.get(100);
			assertNotNull(cacheGet);
			assertTrue(cacheGet.isHit());
			assertFalse(cacheGet.isMiss());
			assertEquals("Hello", cacheGet.getValue());
		}

		{// get at the upper boundary
			CacheGetResult<Object> cacheGet = row.get(199);
			assertNotNull(cacheGet);
			assertTrue(cacheGet.isHit());
			assertFalse(cacheGet.isMiss());
			assertEquals("Hello", cacheGet.getValue());
		}

		{// get below the lower boundary
			CacheGetResult<Object> cacheGet = row.get(99);
			assertNotNull(cacheGet);
			assertTrue(cacheGet.isMiss());
			assertFalse(cacheGet.isHit());
		}

		{ // get above the upper boundary
			CacheGetResult<Object> cacheGet = row.get(200);
			assertNotNull(cacheGet);
			assertTrue(cacheGet.isMiss());
			assertFalse(cacheGet.isHit());
		}
	}

	@Test
	public void leastRecentlyUsedEvictionWorks() {
		UsageRegistry<GetResult<?>> lruRegistry = new RangedGetResultUsageRegistry();
		QualifiedKey rowKey = QualifiedKey.createInDefaultKeyspace("Test");
		MosaicRow row = new MosaicRow(rowKey, lruRegistry, new CacheStatisticsImpl(), this::noSizeChangeHandler);

		GetResult<Object> simulatedGetResult1 = GetResult.create(rowKey, "Hello", Period.createRange(100, 200));
		row.put(simulatedGetResult1);
		GetResult<Object> simulatedGetResult2 = GetResult.create(rowKey, "World", Period.createRange(200, 300));
		row.put(simulatedGetResult2);
		assertEquals(2, row.size());

		// simulate some gets
		// Note: to make the outcome of the test deterministic, we insert 'sleep' calls here. In a real-world
		// scenario, it is of course allowed that several accesses occur on a MosaicRow in the same millisecond,
		// but the result is hard to perform any checks upon.
		this.sleep(1);
		assertEquals("Hello", row.get(150).getValue());
		this.sleep(1);
		assertEquals("World", row.get(250).getValue());
		this.sleep(1);
		assertEquals("Hello", row.get(125).getValue());

		// now, remove the least recently used item (which is the 'simulatedGetResult2')
		lruRegistry.removeLeastRecentlyUsedElement();
		// assert that the element is gone from the LRU registry AND from the row
		assertEquals(1, lruRegistry.sizeInElements());
		assertEquals(1, row.size());

		// assert that queries on 'simulatedGetResult2' are now cache misses
		CacheGetResult<Object> result = row.get(250);
		assertNotNull(result);
		assertTrue(result.isMiss());
		assertFalse(result.isHit());
	}

	@Test
	public void periodsAreArrangedInDescendingOrder() {
		UsageRegistry<GetResult<?>> lruRegistry = FakeUsageRegistry.getInstance();
		QualifiedKey rowKey = QualifiedKey.createInDefaultKeyspace("Test");
		MosaicRowTestSpy row = new MosaicRowTestSpy(rowKey, lruRegistry, new CacheStatisticsImpl(), this::noSizeChangeHandler);

		GetResult<Object> simulatedGetResult1 = GetResult.create(rowKey, "Hello", Period.createRange(100, 200));
		row.put(simulatedGetResult1);
		GetResult<Object> simulatedGetResult2 = GetResult.create(rowKey, "World", Period.createRange(0, 100));
		row.put(simulatedGetResult2);
		GetResult<Object> simulatedGetResult3 = GetResult.create(rowKey, "Foo", Period.createRange(200, 300));
		row.put(simulatedGetResult3);

		Iterator<GetResult<?>> iterator = row.getContents().iterator();
		assertEquals(simulatedGetResult3, iterator.next());
		assertEquals(simulatedGetResult1, iterator.next());
		assertEquals(simulatedGetResult2, iterator.next());

	}

	@Test
	public void writeThroughWorks() {
		RangedGetResultUsageRegistry lruRegistry = new RangedGetResultUsageRegistry();
		QualifiedKey rowKey = QualifiedKey.createInDefaultKeyspace("Test");
		MosaicRow row = new MosaicRow(rowKey, lruRegistry, new CacheStatisticsImpl(), this::noSizeChangeHandler);

		GetResult<Object> simulatedGetResult1 = GetResult.create(rowKey, "Hello", Period.createRange(100, 200));
		row.put(simulatedGetResult1);
		GetResult<Object> simulatedGetResult2 = GetResult.create(rowKey, "World", Period.createOpenEndedRange(200));
		row.put(simulatedGetResult2);
		assertEquals(2, row.size());

		// assert that the result of a cache get is "World" after 300
		assertEquals("World", row.get(350).getValue());

		row.writeThrough(300, "Foo");

		// assert that we have 3 entries now
		assertEquals(3, row.size());

		System.out.println("Before 'get(350)': " + lruRegistry.toDebugString());

		// assert that the result of a cache get is now "Foo" after 300
		assertEquals("Foo", row.get(350).getValue());

		System.out.println("After 'get(350)' : " + lruRegistry.toDebugString());

		// make the "Foo" item the least recently used
		this.sleep(1);
		assertEquals("Hello", row.get(150).getValue());
		System.out.println("After 'get(150)':  " + lruRegistry.toDebugString());
		this.sleep(1);
		assertEquals("World", row.get(250).getValue());
		System.out.println("After 'get(250)':  " + lruRegistry.toDebugString());

		// remove the least recently used items (which is the old unlimited "World" item and the new "Foo" item)
		lruRegistry.removeLeastRecentlyUsedUntil(() -> row.size() <= 2);

		System.out.println("After reduce:      " + lruRegistry.toDebugString());
		System.out.println("Elements in row:   " + row.getContents());

		// assert that a get after 300 now results in a cache miss
		assertTrue(row.get(350).isMiss());
	}

	// =================================================================================================================
	// HELPER METHODS
	// =================================================================================================================

	private void noSizeChangeHandler(final MosaicRow row, final int sizeDelta) {
		// nothing to do
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	private static class MosaicRowTestSpy extends MosaicRow {

		public MosaicRowTestSpy(final QualifiedKey rowKey, final UsageRegistry<GetResult<?>> lruRegistry, final CacheStatisticsImpl statistics, final RowSizeChangeCallback callback) {
			super(rowKey, lruRegistry, statistics, callback);
		}

		@Override
		public Set<GetResult<?>> getContents() {
			return super.getContents();
		}

	}
}
