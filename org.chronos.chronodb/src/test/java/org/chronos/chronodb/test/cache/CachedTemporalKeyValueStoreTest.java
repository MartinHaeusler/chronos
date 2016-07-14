package org.chronos.chronodb.test.cache;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.impl.cache.mosaic.MosaicCache;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class CachedTemporalKeyValueStoreTest extends AllChronoDBBackendsTest {

	@Test
	public void canAssignACacheToATKVS() {
		ChronoDB db = this.getChronoDB();
		TemporalKeyValueStore tkvs = this.getMasterTkvs(db);

		ChronoDBCache cache = new MosaicCache();
		tkvs.setCache(cache);

		assertTrue(tkvs.getCache() == cache);
	}

	@Test
	public void cacheIsFilledByTKVS() {
		ChronoDB db = this.getChronoDB();
		TemporalKeyValueStore tkvs = this.getMasterTkvs(db);
		// initialize the cache
		MosaicCache cache = new MosaicCache();
		tkvs.setCache(cache);
		// perform some usual work
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.put("Foo", "Bar");
		tx.put("Number", 42);
		tx.commit();

		// the commit should have triggered a write-through, and filled the cache with 3 entries
		assertTrue(cache.size() >= 3);
	}

	@Test
	public void cacheProducesHitsOnGetOperations() {
		ChronoDB db = this.getChronoDB();
		TemporalKeyValueStore tkvs = this.getMasterTkvs(db);
		// initialize the cache
		MosaicCache cache = new MosaicCache();
		tkvs.setCache(cache);
		// perform some usual work
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.put("Foo", "Bar");
		tx.put("Number", 42);
		tx.commit();

		// the commit should have triggered a write-through, and filled the cache with 3 entries
		assertTrue(cache.size() >= 3);

		// now, the next requests on "get" should produce cache hits.
		cache.getStatistics().reset();

		assertEquals("World", tx.get("Hello"));
		assertEquals("Bar", tx.get("Foo"));
		assertEquals(42, (int) tx.get("Number"));

		assertEquals(3, cache.getStatistics().getRequestCount());
		assertEquals(3, cache.getStatistics().getCacheHitCount());
		assertEquals(0, cache.getStatistics().getCacheMissCount());
		assertEquals(1.0, cache.getStatistics().getCacheHitRatio(), 0.001);
		assertEquals(0.0, cache.getStatistics().getCacheMissRatio(), 0.001);

	}

	@Test
	public void performingAGetMayTriggerLruEviction() {
		ChronoDB db = this.getChronoDB();
		TemporalKeyValueStore tkvs = this.getMasterTkvs(db);
		// initialize the cache (we intentionally use a size of 2 here)
		MosaicCache cache = new MosaicCache(2);
		tkvs.setCache(cache);
		// perform some usual work
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.put("Foo", "Bar");
		tx.put("Number", 42);
		tx.commit();

		// we completely reset the cache here, because write-through operations on "tx.commit()" also
		// fill the cache. In order to demonstrate that reads will also fill the cache, we need to flush
		// it here.
		cache.getStatistics().reset();
		cache.clear();

		// perform the first two gets
		assertEquals("World", tx.get("Hello"));
		assertEquals("Bar", tx.get("Foo"));

		// we should have a cache size of 2, 2 misses and 0 hits
		assertEquals(2, cache.size());
		assertEquals(2, cache.getStatistics().getCacheMissCount());
		assertEquals(0, cache.getStatistics().getCacheHitCount());

		// now, when performing the "get" on a third key, the LRU eviction should trigger, removing
		// the pair "Hello"->"World"
		assertEquals(42, (int) tx.get("Number"));

		assertEquals(2, cache.size());
		assertEquals(3, cache.getStatistics().getCacheMissCount());
		assertEquals(0, cache.getStatistics().getCacheHitCount());

		// another call to the (now removed) pair should result in another miss (as it was LRU-evicted)
		assertEquals("World", tx.get("Hello"));
		assertEquals(2, cache.size());
		assertEquals(4, cache.getStatistics().getCacheMissCount());
		assertEquals(0, cache.getStatistics().getCacheHitCount());
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void writeThroughWorksCorrectly() {
		ChronoDB db = this.getChronoDB();
		// first, try to get a value for a key that doesn't exist
		Object value = db.tx().get("mykey");
		assertNull(value);
		// the previous operation should have triggered a cache insert
		ChronoDBCache cache = this.getMasterTkvs(db).getCache();
		int cacheEntries = cache.size();
		assertEquals(1, cacheEntries);
		// we should have one cache miss (the one we just produced)
		assertEquals(1, cache.getStatistics().getCacheMissCount());
		// now, perform a commit on the same key
		ChronoDBTransaction tx = db.tx();
		tx.put("mykey", "Hello World!");
		tx.commit();
		// when we now get the value for the key, we should see the real result
		assertEquals("Hello World!", db.tx().get("mykey"));
		// this should have been a cache hit due to write-through
		assertTrue(cache.getStatistics().getCacheHitCount() >= 1);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void writeThroughWorksCorrectlyInIncrementalCommit() {
		ChronoDB db = this.getChronoDB();
		// first, try to get a value for a key that doesn't exist
		Object value = db.tx().get("mykey");
		assertNull(value);
		// the previous operation should have triggered a cache insert
		ChronoDBCache cache = this.getMasterTkvs(db).getCache();
		int cacheEntries = cache.size();
		assertEquals(1, cacheEntries);
		// we should have one cache miss (the one we just produced)
		assertEquals(1, cache.getStatistics().getCacheMissCount());
		// now, perform a commit on the same key
		ChronoDBTransaction tx = db.tx();
		tx.put("mykey", "Hello World!");
		tx.commitIncremental();
		// terminate the incremental commit process
		tx.commit();
		// when we now get the value for the key, we should see the real result
		assertEquals("Hello World!", db.tx().get("mykey"));
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void canWriteThroughCacheSeveralTimesOnSameKeyInIncrementalCommit() {
		ChronoDB db = this.getChronoDB();
		// first, try to get a value for a key that doesn't exist
		Object value = db.tx().get("mykey");
		assertNull(value);
		// the previous operation should have triggered a cache insert
		ChronoDBCache cache = this.getMasterTkvs(db).getCache();
		int cacheEntries = cache.size();
		assertEquals(1, cacheEntries);
		// we should have one cache miss (the one we just produced)
		assertEquals(1, cache.getStatistics().getCacheMissCount());
		// now, we perform an incremental commit on that key
		ChronoDBTransaction tx = db.tx();
		tx.put("mykey", "Hello World!");
		tx.commitIncremental();
		// make sure that the cache contains the correct result
		assertEquals("Hello World!", tx.get("mykey"));
		// also make sure that hte previous query was a cache hit
		assertTrue(cache.getStatistics().getCacheHitCount() >= 1);
		// now, overwrite with another value
		tx.put("mykey", "Foo");
		tx.commitIncremental();
		// make sure that the cache contains the correct result
		assertEquals("Foo", tx.get("mykey"));
		// also make sure that hte previous query was a cache hit
		assertTrue(cache.getStatistics().getCacheHitCount() >= 2);
		// perform the final commit
		tx.commit();
		// make sure that the cache contains the correct result
		assertEquals("Foo", tx.get("mykey"));
	}
}
