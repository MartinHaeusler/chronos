package org.chronos.chronodb.test.engine.indexing;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
import org.chronos.chronodb.test.util.model.payload.NamedPayloadNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class IndexMatchModesTest extends AllChronoDBBackendsTest {

	@Test
	public void testEquals() {
		ChronoDB db = this.getChronoDB();
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
		// positive test: there must be exactly one key-value pair where "name" is equal to "Foo Bar"
		long count = tx.find().inDefaultKeyspace().where("name").isEqualTo("Foo Bar").count();
		assertEquals(1, count);
		// negative test: we are performing EQUALS, so no key-value pair with name "Foo" must exist
		long count2 = tx.find().inDefaultKeyspace().where("name").isEqualTo("Foo").count();
		assertEquals(0, count2);
	}

	@Test
	public void testEqualsIgnoreCase() {
		ChronoDB db = this.getChronoDB();
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
		// positive test: there must be exactly one key-value pair where case-insensitive "name" is equal to "foo bar"
		long count;
		count = tx.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("Foo Bar").count();
		assertEquals(1, count);
		count = tx.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("fOO bAR").count();
		assertEquals(1, count);
		count = tx.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("foo bar").count();
		assertEquals(1, count);
		count = tx.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("FOO BAR").count();
		assertEquals(1, count);
		// negative test: we are performing EQUALS, so no key-value pair with case-insensitive name "foo" must exist
		long count2;
		count2 = tx.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("Foo").count();
		assertEquals(0, count2);
		count2 = tx.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("fOO").count();
		assertEquals(0, count2);
		count2 = tx.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("foo").count();
		assertEquals(0, count2);
		count2 = tx.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("FOO").count();
		assertEquals(0, count2);
	}

	@Test
	public void testNotEquals() {
		ChronoDB db = this.getChronoDB();
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
		long count = tx.find().inDefaultKeyspace().not().where("name").isEqualTo("Foo Bar").count();
		assertEquals(2, count);
		long count2 = tx.find().inDefaultKeyspace().not().where("name").isEqualTo("Foo").count();
		assertEquals(3, count2);
	}

	@Test
	public void testNotEqualsIgnoreCase() {
		ChronoDB db = this.getChronoDB();
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
		long count;
		count = tx.find().inDefaultKeyspace().not().where("name").isEqualToIgnoreCase("Foo Bar").count();
		assertEquals(2, count);
		count = tx.find().inDefaultKeyspace().not().where("name").isEqualToIgnoreCase("fOO bAR").count();
		assertEquals(2, count);
		count = tx.find().inDefaultKeyspace().not().where("name").isEqualToIgnoreCase("foo bar").count();
		assertEquals(2, count);
		count = tx.find().inDefaultKeyspace().not().where("name").isEqualToIgnoreCase("FOO BAR").count();
		assertEquals(2, count);

		long count2;
		count2 = tx.find().inDefaultKeyspace().not().where("name").isEqualToIgnoreCase("Foo").count();
		assertEquals(3, count2);
		count2 = tx.find().inDefaultKeyspace().not().where("name").isEqualToIgnoreCase("fOO").count();
		assertEquals(3, count2);
		count2 = tx.find().inDefaultKeyspace().not().where("name").isEqualToIgnoreCase("foo").count();
		assertEquals(3, count2);
		count2 = tx.find().inDefaultKeyspace().not().where("name").isEqualToIgnoreCase("FOO").count();
		assertEquals(3, count2);
	}

	@Test
	public void testContains() {
		ChronoDB db = this.getChronoDB();
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
		// whole words
		long count = tx.find().inDefaultKeyspace().where("name").contains("Foo").count();
		assertEquals(2, count);
		// partial words
		long count2 = tx.find().inDefaultKeyspace().where("name").contains("oo").count();
		assertEquals(2, count2);
		// at end of word
		long count3 = tx.find().inDefaultKeyspace().where("name").contains("ld").count();
		assertEquals(1, count3);
	}

	@Test
	public void testContainsIgnoreCase() {
		ChronoDB db = this.getChronoDB();
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
		// whole words
		long count;
		count = tx.find().inDefaultKeyspace().where("name").containsIgnoreCase("Foo").count();
		assertEquals(2, count);
		count = tx.find().inDefaultKeyspace().where("name").containsIgnoreCase("fOO").count();
		assertEquals(2, count);
		count = tx.find().inDefaultKeyspace().where("name").containsIgnoreCase("foo").count();
		assertEquals(2, count);
		count = tx.find().inDefaultKeyspace().where("name").containsIgnoreCase("FOO").count();
		assertEquals(2, count);
		// partial words
		long count2;
		count2 = tx.find().inDefaultKeyspace().where("name").containsIgnoreCase("oo").count();
		assertEquals(2, count2);
		count2 = tx.find().inDefaultKeyspace().where("name").containsIgnoreCase("OO").count();
		assertEquals(2, count2);
		count2 = tx.find().inDefaultKeyspace().where("name").containsIgnoreCase("oO").count();
		assertEquals(2, count2);
		count2 = tx.find().inDefaultKeyspace().where("name").containsIgnoreCase("Oo").count();
		assertEquals(2, count2);
		// at end of word
		long count3;
		count3 = tx.find().inDefaultKeyspace().where("name").containsIgnoreCase("ld").count();
		assertEquals(1, count3);
		count3 = tx.find().inDefaultKeyspace().where("name").containsIgnoreCase("LD").count();
		assertEquals(1, count3);
		count3 = tx.find().inDefaultKeyspace().where("name").containsIgnoreCase("lD").count();
		assertEquals(1, count3);
		count3 = tx.find().inDefaultKeyspace().where("name").containsIgnoreCase("Ld").count();
		assertEquals(1, count3);
	}

	@Test
	public void testNotContains() {
		ChronoDB db = this.getChronoDB();
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
		// whole words
		long count = tx.find().inDefaultKeyspace().not().where("name").contains("Foo").count();
		assertEquals(1, count);
		// partial words
		long count2 = tx.find().inDefaultKeyspace().not().where("name").contains("oo").count();
		assertEquals(1, count2);
		// at end of word
		long count3 = tx.find().inDefaultKeyspace().not().where("name").contains("ld").count();
		assertEquals(2, count3);
	}

	@Test
	public void testNotContainsIgnoreCase() {
		ChronoDB db = this.getChronoDB();
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
		// whole words
		long count;
		count = tx.find().inDefaultKeyspace().not().where("name").containsIgnoreCase("Foo").count();
		assertEquals(1, count);
		count = tx.find().inDefaultKeyspace().not().where("name").containsIgnoreCase("fOO").count();
		assertEquals(1, count);
		count = tx.find().inDefaultKeyspace().not().where("name").containsIgnoreCase("foo").count();
		assertEquals(1, count);
		count = tx.find().inDefaultKeyspace().not().where("name").containsIgnoreCase("FOO").count();
		assertEquals(1, count);
		// partial words
		long count2;
		count2 = tx.find().inDefaultKeyspace().not().where("name").containsIgnoreCase("oo").count();
		assertEquals(1, count2);
		count2 = tx.find().inDefaultKeyspace().not().where("name").containsIgnoreCase("oO").count();
		assertEquals(1, count2);
		count2 = tx.find().inDefaultKeyspace().not().where("name").containsIgnoreCase("Oo").count();
		assertEquals(1, count2);
		count2 = tx.find().inDefaultKeyspace().not().where("name").containsIgnoreCase("OO").count();
		assertEquals(1, count2);
		// at end of word
		long count3;
		count3 = tx.find().inDefaultKeyspace().not().where("name").containsIgnoreCase("ld").count();
		assertEquals(2, count3);
		count3 = tx.find().inDefaultKeyspace().not().where("name").containsIgnoreCase("LD").count();
		assertEquals(2, count3);
		count3 = tx.find().inDefaultKeyspace().not().where("name").containsIgnoreCase("Ld").count();
		assertEquals(2, count3);
		count3 = tx.find().inDefaultKeyspace().not().where("name").containsIgnoreCase("lD").count();
		assertEquals(2, count3);
	}

	@Test
	public void testStartsWith() {
		ChronoDB db = this.getChronoDB();
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
		long count = tx.find().inDefaultKeyspace().where("name").startsWith("Foo").count();
		assertEquals(2, count);
		long count2 = tx.find().inDefaultKeyspace().where("name").startsWith("Hello").count();
		assertEquals(1, count2);
	}

	@Test
	public void testStartsWithIgnoreCase() {
		ChronoDB db = this.getChronoDB();
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
		long count;
		count = tx.find().inDefaultKeyspace().where("name").startsWithIgnoreCase("Foo").count();
		assertEquals(2, count);
		count = tx.find().inDefaultKeyspace().where("name").startsWithIgnoreCase("fOO").count();
		assertEquals(2, count);
		count = tx.find().inDefaultKeyspace().where("name").startsWithIgnoreCase("foo").count();
		assertEquals(2, count);
		count = tx.find().inDefaultKeyspace().where("name").startsWithIgnoreCase("FOO").count();
		assertEquals(2, count);

		long count2;
		count2 = tx.find().inDefaultKeyspace().where("name").startsWithIgnoreCase("Hello").count();
		assertEquals(1, count2);
		count2 = tx.find().inDefaultKeyspace().where("name").startsWithIgnoreCase("hELLO").count();
		assertEquals(1, count2);
		count2 = tx.find().inDefaultKeyspace().where("name").startsWithIgnoreCase("hello").count();
		assertEquals(1, count2);
		count2 = tx.find().inDefaultKeyspace().where("name").startsWithIgnoreCase("HELLO").count();
		assertEquals(1, count2);
	}

	@Test
	public void testNotStartsWith() {
		ChronoDB db = this.getChronoDB();
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
		long count = tx.find().inDefaultKeyspace().not().where("name").startsWith("Foo").count();
		assertEquals(1, count);
		long count2 = tx.find().inDefaultKeyspace().not().where("name").startsWith("Hello").count();
		assertEquals(2, count2);
	}

	@Test
	public void testNotStartsWithIgnoreCase() {
		ChronoDB db = this.getChronoDB();
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
		long count;
		count = tx.find().inDefaultKeyspace().not().where("name").startsWithIgnoreCase("Foo").count();
		assertEquals(1, count);
		count = tx.find().inDefaultKeyspace().not().where("name").startsWithIgnoreCase("fOO").count();
		assertEquals(1, count);
		count = tx.find().inDefaultKeyspace().not().where("name").startsWithIgnoreCase("foo").count();
		assertEquals(1, count);
		count = tx.find().inDefaultKeyspace().not().where("name").startsWithIgnoreCase("FOO").count();
		assertEquals(1, count);

		long count2;
		count2 = tx.find().inDefaultKeyspace().not().where("name").startsWithIgnoreCase("Hello").count();
		assertEquals(2, count2);
		count2 = tx.find().inDefaultKeyspace().not().where("name").startsWithIgnoreCase("hELLO").count();
		assertEquals(2, count2);
		count2 = tx.find().inDefaultKeyspace().not().where("name").startsWithIgnoreCase("hello").count();
		assertEquals(2, count2);
		count2 = tx.find().inDefaultKeyspace().not().where("name").startsWithIgnoreCase("HELLO").count();
		assertEquals(2, count2);
	}

	@Test
	public void testEndsWith() {
		ChronoDB db = this.getChronoDB();
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
		long count = tx.find().inDefaultKeyspace().where("name").endsWith("Bar").count();
		assertEquals(1, count);
		long count2 = tx.find().inDefaultKeyspace().where("name").endsWith("World").count();
		assertEquals(1, count2);
	}

	@Test
	public void testEndsWithIgnoreCase() {
		ChronoDB db = this.getChronoDB();
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
		long count;
		count = tx.find().inDefaultKeyspace().where("name").endsWithIgnoreCase("Bar").count();
		assertEquals(1, count);
		count = tx.find().inDefaultKeyspace().where("name").endsWithIgnoreCase("bAR").count();
		assertEquals(1, count);
		count = tx.find().inDefaultKeyspace().where("name").endsWithIgnoreCase("bar").count();
		assertEquals(1, count);
		count = tx.find().inDefaultKeyspace().where("name").endsWithIgnoreCase("BAR").count();
		assertEquals(1, count);
		long count2;
		count2 = tx.find().inDefaultKeyspace().where("name").endsWithIgnoreCase("World").count();
		assertEquals(1, count2);
		count2 = tx.find().inDefaultKeyspace().where("name").endsWithIgnoreCase("wORLD").count();
		assertEquals(1, count2);
		count2 = tx.find().inDefaultKeyspace().where("name").endsWithIgnoreCase("world").count();
		assertEquals(1, count2);
		count2 = tx.find().inDefaultKeyspace().where("name").endsWithIgnoreCase("WORLD").count();
		assertEquals(1, count2);
	}

	@Test
	public void testNotEndsWith() {
		ChronoDB db = this.getChronoDB();
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
		long count = tx.find().inDefaultKeyspace().not().where("name").endsWith("Bar").count();
		assertEquals(2, count);
		long count2 = tx.find().inDefaultKeyspace().not().where("name").endsWith("World").count();
		assertEquals(2, count2);
	}

	@Test
	public void testNotEndsWithIgnoreCase() {
		ChronoDB db = this.getChronoDB();
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
		long count;
		count = tx.find().inDefaultKeyspace().not().where("name").endsWithIgnoreCase("Bar").count();
		assertEquals(2, count);
		count = tx.find().inDefaultKeyspace().not().where("name").endsWithIgnoreCase("bAR").count();
		assertEquals(2, count);
		count = tx.find().inDefaultKeyspace().not().where("name").endsWithIgnoreCase("bar").count();
		assertEquals(2, count);
		count = tx.find().inDefaultKeyspace().not().where("name").endsWithIgnoreCase("BAR").count();
		assertEquals(2, count);
		long count2;
		count2 = tx.find().inDefaultKeyspace().not().where("name").endsWithIgnoreCase("World").count();
		assertEquals(2, count2);
		count2 = tx.find().inDefaultKeyspace().not().where("name").endsWithIgnoreCase("wORLD").count();
		assertEquals(2, count2);
		count2 = tx.find().inDefaultKeyspace().not().where("name").endsWithIgnoreCase("world").count();
		assertEquals(2, count2);
		count2 = tx.find().inDefaultKeyspace().not().where("name").endsWithIgnoreCase("WORLD").count();
		assertEquals(2, count2);
	}

	@Test
	public void testMatchesRegex() {
		ChronoDB db = this.getChronoDB();
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
		long count = tx.find().inDefaultKeyspace().where("name").matchesRegex(".*Ba.").count();
		assertEquals(2, count);
		long count2 = tx.find().inDefaultKeyspace().where("name").matchesRegex("Fo+.*").count();
		assertEquals(2, count2);
	}

	@Test
	public void testMatchesRegexCaseInsensitive() {
		ChronoDB db = this.getChronoDB();
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
		long count = tx.find().inDefaultKeyspace().where("name").matchesRegex("(?i)fo+.*").count();
		assertEquals(2, count);
	}

	@Test
	public void testNotMatchesRegex() {
		ChronoDB db = this.getChronoDB();
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
		long count = tx.find().inDefaultKeyspace().not().where("name").matchesRegex(".*o.*").count();
		assertEquals(0, count);
		long count2 = tx.find().inDefaultKeyspace().not().where("name").matchesRegex("Fo+.*").count();
		assertEquals(1, count2);
	}

}
