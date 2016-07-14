package org.chronos.chronodb.test.engine.indexing;

import static org.junit.Assert.*;

import java.util.Set;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.api.exceptions.UnknownIndexException;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.query.SearchSpecification;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.util.NamedPayload;
import org.chronos.chronodb.test.util.NamedPayloadNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class IndexingTest extends AllChronoDBBackendsTest {

	@Test
	public void indexWritingWorks() {
		ChronoDB db = this.getChronoDB();
		// set up the "name" index
		ChronoIndexer nameIndexer = new NamedPayloadNameIndexer();
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

		// assert that the index is correct
		SearchSpecification searchSpec = SearchSpecification.create("name", Condition.EQUALS, TextMatchMode.STRICT,
				"Hello World");
		Set<ChronoIdentifier> r1 = db.getIndexManager().queryIndex(System.currentTimeMillis(), masterBranch,
				searchSpec);
		assertEquals(1, r1.size());
		assertEquals("np1", r1.iterator().next().getKey());
	}

	@Test
	public void readingNonExistentIndexFailsGracefully() {
		ChronoDB db = this.getChronoDB();
		try {
			Branch masterBranch = db.getBranchManager().getMasterBranch();
			SearchSpecification searchSpec = SearchSpecification.create("shenaningan", Condition.EQUALS,
					TextMatchMode.STRICT, "Hello World");
			db.getIndexManager().queryIndex(System.currentTimeMillis(), masterBranch, searchSpec);
			fail();
		} catch (UnknownIndexException e) {
			// expected
		}
	}

	@Test
	public void renameTest() {
		ChronoDB db = this.getChronoDB();
		// set up the "name" index
		ChronoIndexer nameIndexer = new NamedPayloadNameIndexer();
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

}
