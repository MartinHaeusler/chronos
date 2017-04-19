package org.chronos.chronodb.test.engine.transaction;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitException;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
import org.chronos.chronodb.test.util.model.payload.NamedPayloadNameIndexer;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class IncrementalCommitTest extends AllChronoDBBackendsTest {

	@Test
	public void canCommitIncrementallyMoreThanTwice() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		try {
			tx.put("a", 1);
			tx.commitIncremental();
			assertEquals(1, (int) tx.get("a"));
			tx.put("a", 2);
			tx.commitIncremental();
			assertEquals(2, (int) tx.get("a"));
			tx.put("a", 3);
			tx.commitIncremental();
			assertEquals(3, (int) tx.get("a"));
			tx.commit();
		} finally {
			tx.rollback();
		}
		assertEquals(3, (int) db.tx().get("a"));
	}

	@Test
	public void canCommitIncrementally() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		try {
			tx.put("one", 1);
			tx.put("two", 2);
			tx.put("three", 3);
			tx.commitIncremental();
			tx.put("four", 4);
			tx.put("five", 5);
			tx.put("six", 6);
			tx.commitIncremental();
			tx.put("seven", 7);
			tx.put("eight", 8);
			tx.put("nine", 9);
			tx.commit();
		} finally {
			tx.rollback();
		}
		// assert that the data was written correctly
		assertEquals(9, tx.keySet().size());
	}

	@Test
	public void incrementalCommitTransactionCanReadItsOwnModifications() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		try {
			tx.put("one", 1);
			tx.put("two", 2);
			tx.put("three", 3);
			tx.commitIncremental();
			assertEquals(3, tx.keySet().size());
			tx.put("four", 4);
			tx.put("five", 5);
			tx.put("six", 6);
			tx.commitIncremental();
			assertEquals(6, tx.keySet().size());
			tx.put("seven", 7);
			tx.put("eight", 8);
			tx.put("nine", 9);
			tx.commit();
		} finally {
			tx.rollback();
		}
		// assert that the data was written correctly
		Set<String> keySet = tx.keySet();
		assertEquals(9, keySet.size());
	}

	@Test
	public void incrementalCommitTransactionCanReadItsOwnModificationsInIndexer() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("name", new NamedPayloadNameIndexer());
		ChronoDBTransaction tx = db.tx();
		try {
			tx.put("np1", NamedPayload.create1KB("one"));
			tx.put("np2", NamedPayload.create1KB("two"));
			tx.put("np3", NamedPayload.create1KB("three"));
			tx.commitIncremental();
			assertEquals(3, tx.keySet().size());
			assertEquals(1, tx.find().inDefaultKeyspace().where("name").isEqualTo("one").getKeysAsSet().size());
			assertEquals(2, tx.find().inDefaultKeyspace().where("name").contains("e").getKeysAsSet().size());

			tx.put("np4", NamedPayload.create1KB("four"));
			tx.put("np5", NamedPayload.create1KB("five"));
			tx.put("np6", NamedPayload.create1KB("six"));
			tx.commitIncremental();
			assertEquals(6, tx.keySet().size());
			assertEquals(1, tx.find().inDefaultKeyspace().where("name").isEqualTo("one").getKeysAsSet().size());
			assertEquals(3, tx.find().inDefaultKeyspace().where("name").contains("e").getKeysAsSet().size());

			tx.put("np7", NamedPayload.create1KB("seven"));
			tx.put("np8", NamedPayload.create1KB("eight"));
			tx.put("np9", NamedPayload.create1KB("nine"));
			tx.commit();
		} finally {
			tx.rollback();
		}
		// assert that the data was written correctly
		Set<String> keySet = tx.keySet();
		assertEquals(9, keySet.size());
		assertEquals(1, tx.find().inDefaultKeyspace().where("name").isEqualTo("one").getKeysAsSet().size());
		assertEquals(6, tx.find().inDefaultKeyspace().where("name").contains("e").getKeysAsSet().size());
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
	public void incrementalCommitTransactionCanReadItsOwnModificationsInIndexerWithQueryCaching() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("name", new NamedPayloadNameIndexer());
		ChronoDBTransaction tx = db.tx();
		try {
			tx.put("np1", NamedPayload.create1KB("one"));
			tx.put("np2", NamedPayload.create1KB("two"));
			tx.put("np3", NamedPayload.create1KB("three"));
			tx.commitIncremental();
			assertEquals(3, tx.keySet().size());
			assertEquals(1, tx.find().inDefaultKeyspace().where("name").isEqualTo("one").getKeysAsSet().size());
			assertEquals(2, tx.find().inDefaultKeyspace().where("name").contains("e").getKeysAsSet().size());

			tx.put("np4", NamedPayload.create1KB("four"));
			tx.put("np5", NamedPayload.create1KB("five"));
			tx.put("np6", NamedPayload.create1KB("six"));
			tx.commitIncremental();
			assertEquals(6, tx.keySet().size());
			assertEquals(1, tx.find().inDefaultKeyspace().where("name").isEqualTo("one").getKeysAsSet().size());
			assertEquals(3, tx.find().inDefaultKeyspace().where("name").contains("e").getKeysAsSet().size());

			tx.put("np7", NamedPayload.create1KB("seven"));
			tx.put("np8", NamedPayload.create1KB("eight"));
			tx.put("np9", NamedPayload.create1KB("nine"));
			tx.commit();
		} finally {
			tx.rollback();
		}
		// assert that the data was written correctly
		Set<String> keySet = tx.keySet();
		ChronoLogger.logDebug(keySet.toString());
		assertEquals(9, keySet.size());
		assertEquals(1, tx.find().inDefaultKeyspace().where("name").isEqualTo("one").getKeysAsSet().size());

		Iterator<Entry<QualifiedKey, Object>> qualifiedResult = tx.find().inDefaultKeyspace().where("name")
				.contains("e").getQualifiedResult();
		qualifiedResult.forEachRemaining(entry -> {
			ChronoLogger.logDebug(entry.getKey().toString() + " -> " + entry.getValue());

		});

		assertEquals(6, tx.find().inDefaultKeyspace().where("name").contains("e").getKeysAsSet().size());
	}

	@Test
	public void cannotCommitOnOtherTransactionWhileIncrementalCommitProcessIsRunning() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		try {
			tx.put("one", 1);
			tx.put("two", 2);
			tx.put("three", 3);
			tx.commitIncremental();
			assertEquals(3, tx.keySet().size());
			tx.put("four", 4);
			tx.put("five", 5);
			tx.put("six", 6);
			tx.commitIncremental();
			assertEquals(6, tx.keySet().size());

			// simulate a second transaction
			ChronoDBTransaction tx2 = db.tx();
			tx2.put("thirteen", 13);
			try {
				tx2.commit();
				fail("Managed to commit on other transaction while incremental commit is active!");
			} catch (ChronoDBCommitException expected) {
				// pass
			}

			// continue with the incremental commit
			tx.put("seven", 7);
			tx.put("eight", 8);
			tx.put("nine", 9);
			tx.commit();
		} finally {
			tx.rollback();
		}
		// assert that the data was written correctly
		Set<String> keySet = tx.keySet();
		assertEquals(9, keySet.size());
	}

	@Test
	// enable all the caching to make sure that changes are not visible via caching
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
	public void otherTransactionsCannotSeeChangesPerformedByIncrementalCommits() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		try {
			tx.put("one", 1);
			tx.put("two", 2);
			tx.put("three", 3);
			tx.commitIncremental();
			assertEquals(3, tx.keySet().size());
			tx.put("four", 4);
			tx.put("five", 5);
			tx.put("six", 6);
			tx.commitIncremental();
			assertEquals(6, tx.keySet().size());

			// simulate a second transaction
			ChronoDBTransaction tx2 = db.tx();
			assertFalse(tx2.exists("one"));
			assertFalse(tx2.exists("six"));
			assertNull(tx2.get("one"));
			assertNull(tx2.get("six"));
			assertEquals(0, tx2.keySet().size());

			// continue with the incremental commit
			tx.put("seven", 7);
			tx.put("eight", 8);
			tx.put("nine", 9);
			tx.commit();
		} finally {
			tx.rollback();
		}
		// assert that the data was written correctly
		Set<String> keySet = tx.keySet();
		assertEquals(9, keySet.size());
	}

	@Test
	public void incrementalCommitsAppearAsSingleCommitInHistory() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		try {
			tx.put("one", 1);
			tx.put("two", 2);
			tx.put("three", 3);
			tx.put("alpha", 100);
			tx.commitIncremental();
			tx.put("four", 4);
			tx.put("five", 5);
			tx.put("six", 6);
			tx.put("alpha", 200);
			tx.commitIncremental();
			tx.put("seven", 7);
			tx.put("eight", 8);
			tx.put("nine", 9);
			tx.put("alpha", 300);
			tx.commit();
		} finally {
			tx.rollback();
		}
		// assert that the data was written correctly
		assertEquals(10, tx.keySet().size());
		// assert that alpha was written only once in the versioning history
		Iterator<Long> historyOfAlpha = tx.history("alpha");
		assertEquals(1, Iterators.size(historyOfAlpha));
		// assert that all keys have the same timestamp
		Set<Long> timestamps = Sets.newHashSet();
		for (String key : tx.keySet()) {
			Iterator<Long> history = tx.history(key);
			timestamps.add(Iterators.getOnlyElement(history));
		}
		assertEquals(1, timestamps.size());
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void rollbackDuringIncrementalCommitWorks() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		try {
			tx.put("one", 1);
			tx.put("two", 2);
			tx.put("three", 3);
			tx.commitIncremental();
			tx.put("four", 4);
			tx.put("five", 5);
			tx.put("six", 6);
			tx.commitIncremental();
			// simulate user error
			throw new RuntimeException("User error");
		} catch (RuntimeException expected) {
		} finally {
			tx.rollback();
		}
		// assert that the data was rolled back correctly
		assertEquals(0, tx.keySet().size());
	}

	@Test
	public void canCommitRegularlyAfterCompletedIncrementalCommitProcess() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		try {
			tx.put("one", 1);
			tx.put("two", 2);
			tx.put("three", 3);
			tx.commitIncremental();
			tx.put("four", 4);
			tx.put("five", 5);
			tx.put("six", 6);
			tx.commitIncremental();
			tx.put("seven", 7);
			tx.put("eight", 8);
			tx.put("nine", 9);
			tx.commit();
		} finally {
			tx.rollback();
		}
		// assert that the data was written correctly
		assertEquals(9, tx.keySet().size());

		// can commit on a different transaction
		ChronoDBTransaction tx2 = db.tx();
		tx2.put("fourtytwo", 42);
		tx2.commit();

		// can commit on the same transaction
		tx.put("fourtyseven", 47);
		tx.commit();

		assertEquals(11, tx.keySet().size());
	}

	@Test
	public void canCommitRegularlyAfterCanceledIncrementalCommitProcess() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		try {
			tx.put("one", 1);
			tx.put("two", 2);
			tx.put("three", 3);
			tx.commitIncremental();
			tx.put("four", 4);
			tx.put("five", 5);
			tx.put("six", 6);
			tx.commitIncremental();
			tx.put("seven", 7);
			tx.put("eight", 8);
			tx.put("nine", 9);
		} finally {
			tx.rollback();
		}
		// assert that the data was written correctly
		assertEquals(0, tx.keySet().size());

		// can commit on a different transaction
		ChronoDBTransaction tx2 = db.tx();
		tx2.put("fourtytwo", 42);
		tx2.commit();

		// can commit on the same transaction
		tx.put("fourtyseven", 47);
		tx.commit();

		assertEquals(2, tx.keySet().size());
	}

	@Test
	public void secondaryIndexingIsCorrectDuringIncrementalCommit() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("firstName", new FirstNameIndexer());
		db.getIndexManager().addIndexer("lastName", new LastNameIndexer());
		ChronoDBTransaction tx = db.tx();
		try {
			// add three persons
			tx.put("p1", new Person("John", "Doe"));
			tx.put("p2", new Person("John", "Smith"));
			tx.put("p3", new Person("Jane", "Doe"));

			// perform the incremental commit
			tx.commitIncremental();
			ChronoLogger.logDebug("After 1st commitIncremental");

			// make sure that we can find them
			assertEquals(2, tx.find().inDefaultKeyspace().where("firstName").isEqualToIgnoreCase("john").count());
			assertEquals(2, tx.find().inDefaultKeyspace().where("lastName").isEqualToIgnoreCase("doe").count());

			// change Jane's and John's first names
			tx.put("p3", new Person("Jayne", "Doe"));
			tx.put("p1", new Person("Jack", "Doe"));

			// perform the incremental commit
			tx.commitIncremental();

			ChronoLogger.logDebug("After 2nd commitIncremental");

			// make sure that we can't find John any longer
			assertEquals(1, tx.find().inDefaultKeyspace().where("firstName").isEqualToIgnoreCase("john").count());
			assertEquals(2, tx.find().inDefaultKeyspace().where("lastName").isEqualToIgnoreCase("doe").count());

			// change Jack's first name (yet again)
			tx.put("p1", new Person("Joe", "Doe"));

			// perform the incremental commit
			tx.commitIncremental();

			ChronoLogger.logDebug("After 3rd commitIncremental");

			// make sure that we can't find Jack Doe any longer
			assertEquals(0, tx.find().inDefaultKeyspace().where("firstName").isEqualToIgnoreCase("jack").count());
			// john smith should still be there
			assertEquals(1, tx.find().inDefaultKeyspace().where("firstName").isEqualToIgnoreCase("john").count());
			// we still have jayne doe and joe doe
			assertEquals(2, tx.find().inDefaultKeyspace().where("lastName").isEqualToIgnoreCase("doe").count());
			// jayne should be in the first name index as well
			assertEquals(1, tx.find().inDefaultKeyspace().where("firstName").isEqualToIgnoreCase("jayne").count());

			// delete Joe
			tx.remove("p1");

			// make sure that joe's gone
			assertEquals(0, tx.find().inDefaultKeyspace().where("firstName").isEqualTo("joe").count());

			// do the full commit
			tx.commit();

			ChronoLogger.logDebug("After full commit");

		} finally {
			tx.rollback();
		}

		ChronoLogger.logDebug("TX timestamp is: " + tx.getTimestamp());

		// in the end, there should be john smith and jayne doe
		Set<Object> johns = tx.find().inDefaultKeyspace().where("firstName").isEqualToIgnoreCase("john")
				.getValuesAsSet();
		assertEquals(1, johns.size());

		Set<Object> does = tx.find().inDefaultKeyspace().where("lastName").isEqualToIgnoreCase("doe").getValuesAsSet();
		assertEquals(1, does.size());
	}

	@Test
	public void renamingElementsInSecondaryIndexDuringIncrementalCommitWorks() {
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
		tx2.commitIncremental();
		tx2.put("np1", NamedPayload.create1KB("Renamed"));
		tx2.commitIncremental();
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
	public void multipleIncrementalCommitsOnExistingDataWork() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("firstName", new FirstNameIndexer());
		db.getIndexManager().addIndexer("lastName", new LastNameIndexer());
		ChronoDBTransaction tx = db.tx();
		tx.put("p1", new Person("John", "Doe"));
		tx.put("p2", new Person("Jane", "Doe"));
		tx.commit();
		try {
			// rename "John Doe" to "Jack Doe"
			tx.put("p1", new Person("Jack", "Doe"));
			tx.commitIncremental();
			// assert that the indexer state is correct
			assertEquals(0, tx.find().inDefaultKeyspace().where("firstName").isEqualTo("John").count());
			assertEquals(1, tx.find().inDefaultKeyspace().where("firstName").isEqualTo("Jack").count());
			assertEquals(1, tx.find().inDefaultKeyspace().where("firstName").isEqualTo("Jane").count());

			// rename "Jack Doe" to "Johnny Doe"
			tx.put("p1", new Person("Johnny", "Doe"));
			tx.commitIncremental();
			assertEquals(0, tx.find().inDefaultKeyspace().where("firstName").isEqualTo("John").count());
			assertEquals(0, tx.find().inDefaultKeyspace().where("firstName").isEqualTo("Jack").count());
			assertEquals(1, tx.find().inDefaultKeyspace().where("firstName").isEqualTo("Johnny").count());
			assertEquals(1, tx.find().inDefaultKeyspace().where("firstName").isEqualTo("Jane").count());

			// perform the final commit
			tx.commit();
		} finally {
			tx.rollback();
		}
		// assert that the final result is correct
		assertEquals(0, tx.find().inDefaultKeyspace().where("firstName").isEqualTo("John").count());
		assertEquals(0, tx.find().inDefaultKeyspace().where("firstName").isEqualTo("Jack").count());
		assertEquals(1, tx.find().inDefaultKeyspace().where("firstName").isEqualTo("Johnny").count());
		assertEquals(1, tx.find().inDefaultKeyspace().where("firstName").isEqualTo("Jane").count());
	}

	@Test
	public void canInsertAndRemoveDuringIncrementalCommit() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("name", new NamedPayloadNameIndexer());
		{
			ChronoDBTransaction tx = db.tx();
			tx.put("one", NamedPayload.create1KB("Hello World"));
			tx.commit();
		}
		// make sure that the commit worked
		assertEquals(Sets.newHashSet("one"), db.tx().keySet());
		assertEquals(1, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("hello").count());

		// perform incremental commits
		{
			ChronoDBTransaction tx = db.tx();
			tx.commitIncremental();
			tx.put("two", NamedPayload.create1KB("Foo"));
			tx.put("three", NamedPayload.create1KB("Bar"));
			tx.commitIncremental();
			tx.remove("two");
			tx.put("four", NamedPayload.create1KB("Baz"));
			tx.commitIncremental();
			tx.put("five", NamedPayload.create1KB("John Doe"));
			tx.commit();
		}
		// make sure that the state of the database is consistent
		assertEquals(Sets.newHashSet("one", "three", "four", "five"), db.tx().keySet());
		assertEquals(0, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("foo").count());
		assertEquals(2, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("ba").count());
	}

	@Test
	public void canUpdateAndRemoveDuringIncrementalCommit() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("name", new NamedPayloadNameIndexer());
		{
			ChronoDBTransaction tx = db.tx();
			tx.put("one", NamedPayload.create1KB("Hello World"));
			tx.put("two", NamedPayload.create1KB("Initial State"));
			tx.commit();
		}
		// make sure that the commit worked
		assertEquals(Sets.newHashSet("one", "two"), db.tx().keySet());
		assertEquals(1, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("hello").count());
		assertEquals(1, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("initial").count());

		// perform incremental commits
		{
			ChronoDBTransaction tx = db.tx();
			tx.commitIncremental();
			tx.put("two", NamedPayload.create1KB("Foo"));
			tx.put("three", NamedPayload.create1KB("Bar"));
			tx.commitIncremental();
			tx.remove("two");
			tx.put("four", NamedPayload.create1KB("Baz"));
			tx.commitIncremental();
			tx.put("five", NamedPayload.create1KB("John Doe"));
			tx.commit();
		}
		// make sure that the state of the database is consistent
		assertEquals(Sets.newHashSet("one", "three", "four", "five"), db.tx().keySet());
		assertEquals(0, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("foo").count());
		assertEquals(2, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("ba").count());
	}

	@Test
	public void getAndExistsWorkProperlyAfterDeletionDuringIncrementalCommit() {
		ChronoDB db = this.getChronoDB();
		{ // add some base data
			ChronoDBTransaction tx = db.tx();
			tx.put("a", "Hello");
			tx.put("b", "World");
			tx.commit();
		}
		{ // do the incremental commit process
			ChronoDBTransaction tx = db.tx();
			assertEquals("Hello", tx.get("a"));
			assertEquals("World", tx.get("b"));
			assertTrue(tx.exists("a"));
			assertTrue(tx.exists("b"));

			tx.put("c", "Foo");
			tx.put("d", "Bar");

			tx.commitIncremental();

			assertTrue(tx.exists("a"));
			assertTrue(tx.exists("b"));
			assertTrue(tx.exists("c"));
			assertTrue(tx.exists("d"));

			tx.remove("a");

			tx.commitIncremental();

			assertNull(tx.get("a"));
			assertFalse(tx.exists("a"));

			tx.commit();
		}

		assertNull(db.tx().get("a"));
		assertFalse(db.tx().exists("a"));
	}

	private static class Person {

		private String firstName;
		private String lastName;

		@SuppressWarnings("unused")
		public Person() {
			// default constructor for kryo
			this(null, null);
		}

		public Person(final String firstName, final String lastName) {
			this.firstName = firstName;
			this.lastName = lastName;
		}

		public String getFirstName() {
			return this.firstName;
		}

		public String getLastName() {
			return this.lastName;
		}

		@Override
		public String toString() {
			return "Person[" + this.firstName + " " + this.lastName + "]";
		}
	}

	private static class FirstNameIndexer implements ChronoIndexer {

		@Override
		public boolean canIndex(final Object object) {
			return object instanceof Person;
		}

		@Override
		public Set<String> getIndexValues(final Object object) {
			Person person = (Person) object;
			return Collections.singleton(person.getFirstName());
		}

	}

	private static class LastNameIndexer implements ChronoIndexer {

		@Override
		public boolean canIndex(final Object object) {
			return object instanceof Person;
		}

		@Override
		public Set<String> getIndexValues(final Object object) {
			Person person = (Person) object;
			return Collections.singleton(person.getLastName());
		}

	}
}
