package org.chronos.chronodb.test.cache.query;

import static org.junit.Assert.*;

import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.impl.index.StandardIndexManager;
import org.chronos.chronodb.internal.impl.index.querycache.ChronoIndexQueryCache;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronodb.test.util.NamedPayload;
import org.chronos.chronodb.test.util.NamedPayloadNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class QueryCacheTest extends AllChronoDBBackendsTest {

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "10")
	public void cachingQueriesWorks() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("name", new NamedPayloadNameIndexer());
		ChronoDBTransaction tx = db.tx();
		tx.put("np1", NamedPayload.create1KB("Hello"));
		tx.put("np2", NamedPayload.create1KB("World"));
		tx.put("np3", NamedPayload.create1KB("Foo"));
		tx.put("np4", NamedPayload.create1KB("Bar"));
		tx.put("np5", NamedPayload.create1KB("Baz"));
		tx.commit();

		// get the query cache instance
		StandardIndexManager indexManager = (StandardIndexManager) db.getIndexManager();
		ChronoIndexQueryCache queryCache = indexManager.getIndexQueryCache();
		// make sure that the query cache is in place
		assertNotNull(queryCache);
		// make sure that the cache is configured to record statistics
		assertNotNull(queryCache.getStats());

		// initially, the cache stats should be empty
		assertEquals(0, queryCache.getStats().hitCount());
		assertEquals(0, queryCache.getStats().missCount());

		// run the first query
		Set<QualifiedKey> set = tx.find().inDefaultKeyspace().where("name").contains("o").getKeysAsSet();

		// the first query should result in a cache miss
		assertEquals(0, queryCache.getStats().hitCount());
		assertEquals(1, queryCache.getStats().missCount());

		// run it again
		Set<QualifiedKey> set2 = tx.find().inDefaultKeyspace().where("name").contains("o").getKeysAsSet();

		// the second query should result in a cache hit
		assertEquals(1, queryCache.getStats().hitCount());
		assertEquals(1, queryCache.getStats().missCount());

		// assert that the sets are the same
		assertEquals(set, set2);

		Set<String> keys = set.stream().map(entry -> entry.getKey()).collect(Collectors.toSet());
		assertEquals(3, keys.size());
		assertTrue(keys.contains("np1")); // hell[o]
		assertTrue(keys.contains("np2")); // w[o]rld
		assertTrue(keys.contains("np3")); // f[o]o
	}

}
