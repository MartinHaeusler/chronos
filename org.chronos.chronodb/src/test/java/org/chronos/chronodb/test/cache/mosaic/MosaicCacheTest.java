package org.chronos.chronodb.test.cache.mosaic;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.RangedGetResult;
import org.chronos.chronodb.internal.api.cache.CacheGetResult;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.impl.cache.mosaic.MosaicCache;
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
		ChronoDBCache cache = new MosaicCache();

		QualifiedKey key = QualifiedKey.createInDefaultKeyspace("Hello");

		cache.cache(key, RangedGetResult.create(key, "World", Period.createRange(100, 200)));
		cache.cache(key, RangedGetResult.create(key, "Foo", Period.createRange(200, 500)));

		{ // below lowest entry
			CacheGetResult<Object> result = cache.get(99, key);
			assertNotNull(result);
			assertTrue(result.isMiss());
		}

		{ // at lower bound of lowest entry
			CacheGetResult<Object> result = cache.get(100, key);
			assertNotNull(result);
			assertTrue(result.isHit());
			assertEquals("World", result.getValue());
		}

		{ // in between bounds
			CacheGetResult<Object> result = cache.get(150, key);
			assertNotNull(result);
			assertTrue(result.isHit());
			assertEquals("World", result.getValue());
		}

		{ // at upper bound of lowest entry
			CacheGetResult<Object> result = cache.get(199, key);
			assertNotNull(result);
			assertTrue(result.isHit());
			assertEquals("World", result.getValue());
		}

		{ // at lower bound of upper entry
			CacheGetResult<Object> result = cache.get(200, key);
			assertNotNull(result);
			assertTrue(result.isHit());
			assertEquals("Foo", result.getValue());
		}

		{ // in between bounds
			CacheGetResult<Object> result = cache.get(300, key);
			assertNotNull(result);
			assertTrue(result.isHit());
			assertEquals("Foo", result.getValue());
		}

		{ // at upper bound of upper entry
			CacheGetResult<Object> result = cache.get(499, key);
			assertNotNull(result);
			assertTrue(result.isHit());
			assertEquals("Foo", result.getValue());
		}

		{ // outside of any entry
			CacheGetResult<Object> result = cache.get(550, key);
			assertNotNull(result);
			assertTrue(result.isMiss());
		}

	}

	@Test
	public void cacheGetOnNonExistingRowDoesntCrash() {
		ChronoDBCache cache = new MosaicCache();
		CacheGetResult<Object> result = cache.get(1234, QualifiedKey.createInDefaultKeyspace("Fake"));
		assertNotNull(result);
		assertTrue(result.isMiss());
	}

	@Test
	public void leastRecentlyUsedShrinkOnCacheBehaviourWorks() {
		ChronoDBCache cache = new MosaicCache(1);

		QualifiedKey key = QualifiedKey.createInDefaultKeyspace("Hello");

		cache.cache(key, RangedGetResult.create(key, "World", Period.createRange(100, 200)));
		cache.cache(key, RangedGetResult.create(key, "Foo", Period.createRange(200, 300)));

		assertTrue(cache.get(250, key).isHit());
		assertEquals("Foo", cache.get(250, key).getValue());

		assertFalse(cache.get(150, key).isHit());
	}

	@Test
	public void leastRecentlyUsedShrinkOnWriteThroughBehaviourWorks() {
		ChronoDBCache cache = new MosaicCache(1);

		QualifiedKey key = QualifiedKey.createInDefaultKeyspace("Hello");

		cache.cache(key, RangedGetResult.create(key, "World", Period.createOpenEndedRange(100)));
		cache.writeThrough(200, key, "Foo");

		assertTrue(cache.get(250, key).isHit());
		assertEquals("Foo", cache.get(250, key).getValue());

		assertFalse(cache.get(150, key).isHit());
	}

}
