package org.chronos.chronodb.test.cache.mosaic;

import static org.junit.Assert.*;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.cache.CacheGetResult;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.impl.cache.mosaic.MosaicCache;
import org.chronos.chronodb.test.util.TestUtils;
import org.chronos.common.test.ChronosUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class MosaicCacheTest extends ChronosUnitTest {

	@Test
	public void canCreateMosaicCacheInstance() {
		ChronoDBCache cache = new MosaicCache();
		assertNotNull(cache);
	}

	@Test
	public void cacheAndGetAreConsistent() {
		ChronoDBCache cache = createCacheOfSize(100);

		String branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;

		QualifiedKey key = QualifiedKey.createInDefaultKeyspace("Hello");

		cache.cache(branch, GetResult.create(key, "World", Period.createRange(100, 200)));
		cache.cache(branch, GetResult.create(key, "Foo", Period.createRange(200, 500)));

		{ // below lowest entry
			CacheGetResult<Object> result = cache.get(branch, 99, key);
			assertNotNull(result);
			assertTrue(result.isMiss());
		}

		{ // at lower bound of lowest entry
			CacheGetResult<Object> result = cache.get(branch, 100, key);
			assertNotNull(result);
			assertTrue(result.isHit());
			assertEquals("World", result.getValue());
		}

		{ // in between bounds
			CacheGetResult<Object> result = cache.get(branch, 150, key);
			assertNotNull(result);
			assertTrue(result.isHit());
			assertEquals("World", result.getValue());
		}

		{ // at upper bound of lowest entry
			CacheGetResult<Object> result = cache.get(branch, 199, key);
			assertNotNull(result);
			assertTrue(result.isHit());
			assertEquals("World", result.getValue());
		}

		{ // at lower bound of upper entry
			CacheGetResult<Object> result = cache.get(branch, 200, key);
			assertNotNull(result);
			assertTrue(result.isHit());
			assertEquals("Foo", result.getValue());
		}

		{ // in between bounds
			CacheGetResult<Object> result = cache.get(branch, 300, key);
			assertNotNull(result);
			assertTrue(result.isHit());
			assertEquals("Foo", result.getValue());
		}

		{ // at upper bound of upper entry
			CacheGetResult<Object> result = cache.get(branch, 499, key);
			assertNotNull(result);
			assertTrue(result.isHit());
			assertEquals("Foo", result.getValue());
		}

		{ // outside of any entry
			CacheGetResult<Object> result = cache.get(branch, 550, key);
			assertNotNull(result);
			assertTrue(result.isMiss());
		}

	}

	@Test
	public void cacheGetOnNonExistingRowDoesntCrash() {
		ChronoDBCache cache = createCacheOfSize(1);
		String branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
		CacheGetResult<Object> result = cache.get(branch, 1234, QualifiedKey.createInDefaultKeyspace("Fake"));
		assertNotNull(result);
		assertTrue(result.isMiss());
	}

	@Test
	public void leastRecentlyUsedShrinkOnCacheBehaviourWorks() {
		QualifiedKey key = QualifiedKey.createInDefaultKeyspace("Hello");
		GetResult<?> result1 = GetResult.create(key, "World", Period.createRange(100, 200));
		GetResult<?> result2 = GetResult.create(key, "Foo", Period.createRange(200, 300));

		ChronoDBCache cache = createCacheOfSize(1);

		String branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;

		cache.cache(branch, result1);
		cache.cache(branch, result2);

		assertTrue(cache.get(branch, 250, key).isHit());
		assertEquals("Foo", cache.get(branch, 250, key).getValue());

		assertFalse(cache.get(branch, 150, key).isHit());
	}

	@Test
	public void leastRecentlyUsedShrinkOnWriteThroughBehaviourWorks() {
		QualifiedKey key = QualifiedKey.createInDefaultKeyspace("Hello");
		GetResult<?> result1 = GetResult.create(key, "World", Period.createOpenEndedRange(100));

		ChronoDBCache cache = createCacheOfSize(1);

		String branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;

		cache.cache(branch, result1);
		cache.writeThrough(branch, 200, key, "Foo");

		assertTrue(cache.get(branch, 250, key).isHit());
		assertEquals("Foo", cache.get(branch, 250, key).getValue());

		assertFalse(cache.get(branch, 150, key).isHit());
	}

	@Test
	public void cacheSizeIsCorrect() {
		QualifiedKey qKeyA = QualifiedKey.createInDefaultKeyspace("a");
		QualifiedKey qKeyB = QualifiedKey.createInDefaultKeyspace("b");
		QualifiedKey qKeyC = QualifiedKey.createInDefaultKeyspace("c");
		QualifiedKey qKeyD = QualifiedKey.createInDefaultKeyspace("d");

		ChronoDBCache cache = createCacheOfSize(3);
		cache.cache("master", GetResult.create(qKeyA, "Hello", Period.createRange(100, 200)));
		cache.cache("master", GetResult.create(qKeyA, "World", Period.createRange(200, 300)));
		cache.cache("master", GetResult.create(qKeyA, "Foo", Period.createRange(300, 400)));
		cache.cache("master", GetResult.create(qKeyA, "Bar", Period.createRange(400, 500)));
		cache.cache("master", GetResult.create(qKeyB, "Hello", Period.createRange(100, 200)));
		cache.cache("master", GetResult.create(qKeyB, "World", Period.createRange(200, 300)));
		cache.clear();
		assertEquals(0, cache.size());
		cache.cache("master", GetResult.create(qKeyA, "Hello", Period.createRange(100, 200)));
		cache.cache("master", GetResult.create(qKeyA, "World", Period.createRange(200, 300)));
		cache.cache("master", GetResult.create(qKeyA, "Foo", Period.createRange(300, 400)));
		cache.writeThrough("master", 400, qKeyA, "Bar");
		cache.writeThrough("master", 0, qKeyB, "Hello");
		cache.writeThrough("master", 100, qKeyB, "World");
		cache.writeThrough("master", 100, qKeyC, "World");
		cache.writeThrough("master", 200, qKeyC, "Foo");
		cache.cache("master", GetResult.create(qKeyC, "Bar", Period.createRange(300, 400)));
		cache.writeThrough("master", 100, qKeyD, "World");
		cache.writeThrough("master", 200, qKeyD, "Foo");
		cache.cache("master", GetResult.create(qKeyD, "Bar", Period.createRange(300, 400)));
		cache.rollbackToTimestamp(0);
	}

	// =================================================================================================================
	// HELPER METHODS
	// =================================================================================================================

	private static ChronoDBCache createCacheOfSize(final int size) {
		checkArgument(size > 0, "Precondition violation - argument 'size' must be > 0!");
		final MosaicCache mosaicCache = new MosaicCache(size);
		// we don't use the mosaic cache directly. Instead, we wrap it in a delegating proxy which will
		// assert that all cache invariants are valid before and after every method call.
		ChronoDBCache invariantCheckingProxy = TestUtils.createProxy(ChronoDBCache.class, (self, method, args) -> {
			System.out.println("Before");
			assertCacheInvariants(mosaicCache);
			Object result = method.invoke(mosaicCache, args);
			System.out.println("After");
			assertCacheInvariants(mosaicCache);
			return result;
		});
		return invariantCheckingProxy;
	}

	private static void assertCacheInvariants(final MosaicCache cache) {
		System.out.println("CACHE VALIDATION");
		System.out.println("\tMax Size:       " + cache.maxSize());
		System.out.println("\tCurrent Size:   " + cache.size());
		System.out.println("\tComputed Size:  " + cache.computedSize());
		System.out.println("\tRow Count:      " + cache.rowCount());
		System.out.println("\tListener Count: " + cache.lruListenerCount());
		int maxSize = cache.maxSize();
		int size = cache.size();
		int compSize = cache.computedSize();
		int rowCount = cache.rowCount();
		int listeners = cache.lruListenerCount();
		if (maxSize > 0) {
			assertTrue(size <= maxSize);
		}
		assertEquals(compSize, size);
		assertTrue(rowCount <= compSize);
		assertEquals(rowCount, listeners);
	}
}
