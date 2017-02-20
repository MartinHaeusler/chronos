package org.chronos.chronodb.test.cache;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class MutableValuesCacheTest extends AllChronoDBBackendsTest {

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100000")
	@InstantiateChronosWith(property = ChronoDBConfiguration.ASSUME_CACHE_VALUES_ARE_IMMUTABLE, value = "true")
	public void passingMutableObjectsToTheCacheAsValuesCanModifyItsState() {
		// note that we ENABLE the immutability assumption, even though the values we pass into
		// the cache are NOT immutable. This test case serves as a demonstration that the issue
		// does indeed exist.
		ChronoDB db = this.getChronoDB();
		assertNotNull(db);
		try {
			// assert that we do have a cache
			ChronoDBCache cache = db.getCache();
			assertNotNull(cache);
			MyMutableObject obj1 = new MyMutableObject(1);
			MyMutableObject obj2 = new MyMutableObject(2);
			ChronoDBTransaction tx = db.tx();
			tx.put("one", obj1);
			tx.put("two", obj2);
			tx.commit();

			// assert that a write-through has indeed occurred (i.e. our cache is filled)
			// 2 entries from our commit + 2 entries with NULL value from the indexing
			assertEquals(4, cache.size());
			// now we change the values (which also reside in the cache)
			obj1.setNumber(42);

			// when we now request that element, we should get the (unintentionally) modified version that
			// was never committed to the database. This is of course wrong, but remember that this test
			// only demonstrates the existence of the issue, and we deliberately deactivated the safety
			// measure in the database setup above.
			assertEquals(42, ((MyMutableObject) tx.get("one")).getNumber());
		} finally {
			db.close();
		}
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10000")
	@InstantiateChronosWith(property = ChronoDBConfiguration.ASSUME_CACHE_VALUES_ARE_IMMUTABLE, value = "false")
	public void disablingCachedValuesImmutabilityAssumptionPreventsCacheStateCorruption() {
		// we disable the assumption that cache values are immutable here. It's actually the default,
		// but just to make it explicit we do it here, because that's what this test is all about.
		ChronoDB db = this.getChronoDB();
		assertNotNull(db);
		try {
			// assert that we do have a cache
			ChronoDBCache cache = db.getCache();
			assertNotNull(cache);
			MyMutableObject obj1 = new MyMutableObject(1);
			MyMutableObject obj2 = new MyMutableObject(2);
			ChronoDBTransaction tx = db.tx();
			tx.put("one", obj1);
			tx.put("two", obj2);
			tx.commit();

			// assert that a write-through has indeed occurred (i.e. our cache is filled)
			// 2 entries from our commit + 2 entries with NULL value from the indexing
			assertEquals(4, cache.size());
			// now we change the values (which should NOT alter our cache state)
			obj1.setNumber(42);

			// when we now request that element, we should get the original, ummodified version of the element
			assertEquals(1, ((MyMutableObject) tx.get("one")).getNumber());
		} finally {
			db.close();
		}
	}

	private static class MyMutableObject {

		private int number;

		@SuppressWarnings("unused")
		protected MyMutableObject() {
			// default constructor for serialization purposes
			this(0);
		}

		public MyMutableObject(final int number) {
			this.number = number;
		}

		public int getNumber() {
			return this.number;
		}

		public void setNumber(final int number) {
			this.number = number;
		}

	}

}
