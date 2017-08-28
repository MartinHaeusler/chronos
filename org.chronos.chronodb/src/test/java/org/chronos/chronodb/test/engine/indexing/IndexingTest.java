package org.chronos.chronodb.test.engine.indexing;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.exceptions.ChronoDBIndexingException;
import org.chronos.chronodb.api.exceptions.UnknownIndexException;
import org.chronos.chronodb.api.indexing.DoubleIndexer;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.api.indexing.LongIndexer;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
import org.chronos.chronodb.test.util.model.payload.NamedPayloadNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class IndexingTest extends AllChronoDBBackendsTest {

	@Test
	public void indexWritingWorks() {
		ChronoDB db = this.getChronoDB();
		// set up the "name" index
		StringIndexer nameIndexer = new NamedPayloadNameIndexer();
		db.getIndexManager().addIndexer("name", nameIndexer);
		db.getIndexManager().reindexAll();

		// generate and insert test data
		NamedPayload np1 = NamedPayload.create1KB("Hello World");
		NamedPayload np2 = NamedPayload.create1KB("Foo Bar");
		NamedPayload np3 = NamedPayload.create1KB("Foo Baz");
		ChronoDBTransaction tx = db.tx();
		tx.put("np1", np1);
		tx.put("np2", np2);
		tx.put("np3", np3);
		tx.commit();

		Branch masterBranch = db.getBranchManager().getMasterBranch();
		String defaultKeyspace = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;

		// assert that the index is correct
		SearchSpecification<?> searchSpec = StringSearchSpecification.create("name", Condition.EQUALS, TextMatchMode.STRICT,
				"Hello World");
		Set<String> r1 = db.getIndexManager().queryIndex(System.currentTimeMillis(), masterBranch, defaultKeyspace,
				searchSpec);
		assertEquals(1, r1.size());
		assertEquals("np1", r1.iterator().next());
	}

	@Test
	public void readingNonExistentIndexFailsGracefully() {
		ChronoDB db = this.getChronoDB();
		try {
			Branch masterBranch = db.getBranchManager().getMasterBranch();
			String defaultKeyspace = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;
			SearchSpecification<?> searchSpec = StringSearchSpecification.create("shenaningan", Condition.EQUALS,
					TextMatchMode.STRICT, "Hello World");
			db.getIndexManager().queryIndex(System.currentTimeMillis(), masterBranch, defaultKeyspace, searchSpec);
			fail();
		} catch (UnknownIndexException e) {
			// expected
		}
	}

	@Test
	public void renameTest() {
		ChronoDB db = this.getChronoDB();
		// set up the "name" index
		StringIndexer nameIndexer = new NamedPayloadNameIndexer();
		db.getIndexManager().addIndexer("name", nameIndexer);
		db.getIndexManager().reindexAll();
		// generate and insert test data
		NamedPayload np1 = NamedPayload.create1KB("Hello World");
		NamedPayload np2 = NamedPayload.create1KB("Foo Bar");
		NamedPayload np3 = NamedPayload.create1KB("Foo Baz");
		ChronoDBTransaction tx = db.tx();
		tx.put("np1", np1);
		tx.put("np2", np2);
		tx.put("np3", np3);
		tx.commit();

		ChronoDBTransaction tx2 = db.tx();
		tx2.put("np1", NamedPayload.create1KB("Renamed"));
		tx2.commit();

		// check that we can find the renamed element by its new name
		assertEquals(1, db.tx().find().inDefaultKeyspace().where("name").isEqualTo("Renamed").count());
		// check that we cannot find the renamed element by its old name anymore
		assertEquals(0, db.tx().find().inDefaultKeyspace().where("name").isEqualTo("Hello World").count());
		// in the past, we should still find the non-renamed version
		assertEquals(1, tx.find().inDefaultKeyspace().where("name").isEqualTo("Hello World").count());
		// in the past, we should not find the renamed version
		assertEquals(0, tx.find().inDefaultKeyspace().where("name").isEqualTo("Renamed").count());
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
	public void deleteTest() {
		ChronoDB db = this.getChronoDB();
		// set up the "name" index
		StringIndexer nameIndexer = new NamedPayloadNameIndexer();
		db.getIndexManager().addIndexer("name", nameIndexer);
		db.getIndexManager().reindexAll();
		// generate and insert test data
		NamedPayload np1 = NamedPayload.create1KB("np1");
		NamedPayload np2 = NamedPayload.create1KB("np2");
		NamedPayload np3 = NamedPayload.create1KB("np3");
		ChronoDBTransaction tx = db.tx();
		tx.put("np1", np1);
		tx.put("np2", np2);
		tx.put("np3", np3);
		tx.commit();

		ChronoDBTransaction tx2 = db.tx();
		assertEquals(3, db.tx().find().inDefaultKeyspace().where("name").startsWithIgnoreCase("np").count());
		tx2.remove("np1");
		tx2.remove("np3");
		tx2.commit();

		assertEquals(1, db.tx().find().inDefaultKeyspace().where("name").startsWithIgnoreCase("np").count());
		assertEquals(Collections.singleton("np2"),
				db.tx().find().inDefaultKeyspace().where("name").startsWithIgnoreCase("np").getKeysAsSet().stream()
						.map(qKey -> qKey.getKey()).collect(Collectors.toSet()));

	}

	@Test
	public void attemptingToMixIndexerTypesShouldThrowAnException() {
		ChronoDB db = this.getChronoDB();
		this.assertAddingSecondIndexerFails(db, new DummyStringIndexer(), new DummyLongIndexer());
		db.getIndexManager().clearAllIndices();
		this.assertAddingSecondIndexerFails(db, new DummyStringIndexer(), new DummyDoubleIndexer());
		db.getIndexManager().clearAllIndices();
		this.assertAddingSecondIndexerFails(db, new DummyLongIndexer(), new DummyStringIndexer());
		db.getIndexManager().clearAllIndices();
		this.assertAddingSecondIndexerFails(db, new DummyLongIndexer(), new DummyDoubleIndexer());
		db.getIndexManager().clearAllIndices();
		this.assertAddingSecondIndexerFails(db, new DummyDoubleIndexer(), new DummyStringIndexer());
		db.getIndexManager().clearAllIndices();
		this.assertAddingSecondIndexerFails(db, new DummyDoubleIndexer(), new DummyLongIndexer());
	}

	@Test
	public void canDropAllIndices() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("name", new DummyStringIndexer());
		db.getIndexManager().addIndexer("test", new DummyStringIndexer());
		assertEquals(2, db.getIndexManager().getIndexers().size());
		db.getIndexManager().clearAllIndices();
		assertEquals(0, db.getIndexManager().getIndexers().size());
	}

	private void assertAddingSecondIndexerFails(final ChronoDB db, final Indexer<?> indexer1, final Indexer<?> indexer2) {
		db.getIndexManager().addIndexer("test", indexer1);
		try {
			db.getIndexManager().addIndexer("test", indexer2);
			fail("Managed to mix indexer classes " + indexer1.getClass().getSimpleName() + " and " + indexer2.getClass().getName() + " in same index!");
		} catch (ChronoDBIndexingException expected) {
			// pass
		}
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private static class DummyStringIndexer implements StringIndexer {
		@Override
		public boolean canIndex(final Object object) {
			return true;
		}

		@Override
		public Set<String> getIndexValues(final Object object) {
			return Collections.singleton(String.valueOf(object));
		}
	}

	private static class DummyLongIndexer implements LongIndexer {
		@Override
		public boolean canIndex(final Object object) {
			return true;
		}

		@Override
		public Set<Long> getIndexValues(final Object object) {
			return Collections.singleton((long) String.valueOf(object).length());
		}
	}

	private static class DummyDoubleIndexer implements DoubleIndexer {
		@Override
		public boolean canIndex(final Object object) {
			return true;
		}

		@Override
		public Set<Double> getIndexValues(final Object object) {
			return Collections.singleton((double) String.valueOf(object).length());
		}
	}
}
