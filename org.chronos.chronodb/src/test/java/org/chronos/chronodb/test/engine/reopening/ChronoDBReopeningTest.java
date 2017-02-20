package org.chronos.chronodb.test.engine.reopening;

import static org.junit.Assert.*;

import java.util.Set;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronodb.test.base.AllBackendsTest.DontRunWithBackend;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronodb.test.util.model.payload.NamedPayloadNameIndexer;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
// these tests do not make sense with non-persistent backends.
@DontRunWithBackend({ ChronosBackend.INMEMORY, ChronosBackend.JDBC })
public class ChronoDBReopeningTest extends AllChronoDBBackendsTest {

	@Test
	public void reopeningChronoDbWorks() {
		ChronoDB db = this.getChronoDB();
		assertNotNull(db);
		ChronoDB db2 = this.closeAndReopenDB();
		assertNotNull(db2);
		assertTrue(db != db2);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10000")
	public void reopeningChronoDbPreservesConfiguration() {
		ChronoDB db = this.getChronoDB();
		assertNotNull(db);
		// assert that the settings from the test annotations were applied correctly
		assertTrue(db.getConfiguration().isCachingEnabled());
		assertEquals(10000L, (long) db.getConfiguration().getCacheMaxSize());
		// reinstantiate the DB
		ChronoDB db2 = this.closeAndReopenDB();
		assertNotNull(db2);
		// assert that the settings have carried over
		assertTrue(db2.getConfiguration().isCachingEnabled());
		assertEquals(10000L, (long) db.getConfiguration().getCacheMaxSize());
	}

	@Test
	public void reopeningChronoDbPreservesContents() {
		ChronoDB db = this.getChronoDB();
		assertNotNull(db);
		// fill the db with some data
		ChronoDBTransaction tx = db.tx();
		tx.put("k1", "Hello World");
		tx.put("k2", "Foo Bar");
		tx.put("k3", 42);
		tx.commit();
		// reinstantiate
		ChronoLogger.log("Reinstantiating DB");
		ChronoDB db2 = this.closeAndReopenDB();
		// check that the content is still there
		ChronoDBTransaction tx2 = db2.tx();
		Set<String> keySet = tx2.keySet();
		assertEquals(3, keySet.size());
		assertTrue(keySet.contains("k1"));
		assertTrue(keySet.contains("k2"));
		assertTrue(keySet.contains("k3"));
		assertEquals("Hello World", tx2.get("k1"));
		assertEquals("Foo Bar", tx2.get("k2"));
		assertEquals(42, (int) tx2.get("k3"));
	}

	@Test
	public void reopeningChronoDbPreservesIndexers() {
		ChronoDB db = this.getChronoDB();
		assertNotNull(db);
		// add some indexers
		db.getIndexManager().addIndexer("name", new NamedPayloadNameIndexer());
		db.getIndexManager().addIndexer("test", new NamedPayloadNameIndexer());
		// assert that the indices are present
		assertTrue(db.getIndexManager().getIndexNames().contains("name"));
		assertTrue(db.getIndexManager().getIndexNames().contains("test"));
		assertEquals(2, db.getIndexManager().getIndexNames().size());

		// reinstantiate the DB
		ChronoDB db2 = this.closeAndReopenDB();
		// assert that the indices are still present
		assertTrue(db2.getIndexManager().getIndexNames().contains("name"));
		assertTrue(db2.getIndexManager().getIndexNames().contains("test"));
		assertEquals(2, db2.getIndexManager().getIndexNames().size());
	}

	@Test
	public void reopeningChronoDbPreservesIndexDirtyFlags() {
		ChronoDB db = this.getChronoDB();
		assertNotNull(db);
		// add some indexers
		db.getIndexManager().addIndexer("name", new NamedPayloadNameIndexer());
		db.getIndexManager().addIndexer("test", new NamedPayloadNameIndexer());
		// make sure that 'name' isn't dirty anymore
		db.getIndexManager().reindex("name");
		assertFalse(db.getIndexManager().getDirtyIndices().contains("name"));
		// ... but the 'test' index should still be dirty
		assertTrue(db.getIndexManager().getDirtyIndices().contains("test"));

		// reopen the db
		ChronoDB db2 = this.closeAndReopenDB();

		// assert that the 'name' index is not dirty, but the 'test' index is
		assertFalse(db2.getIndexManager().getDirtyIndices().contains("name"));
		assertTrue(db2.getIndexManager().getDirtyIndices().contains("test"));
	}
}
