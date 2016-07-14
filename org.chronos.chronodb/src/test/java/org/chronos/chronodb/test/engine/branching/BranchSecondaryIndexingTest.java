package org.chronos.chronodb.test.engine.branching;

import static org.junit.Assert.*;

import java.util.Set;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class BranchSecondaryIndexingTest extends AllChronoDBBackendsTest {

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
	public void canAddMultiplicityManyValuesInBranch() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("name", new TestObjectNameIndexer());
		db.getIndexManager().reindexAll();

		ChronoDBTransaction tx1 = db.tx();
		tx1.put("one", new TestObject("One", "TO_One"));
		tx1.put("two", new TestObject("Two", "TO_Two"));
		tx1.commit();

		// assert that we can now find these objects
		assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("One").count());
		assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("TO_One").count());
		assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("Two").count());
		assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("TO_Two").count());

		// create a branch
		Branch branch = db.getBranchManager().createBranch("MyBranch");
		assertNotNull(branch);
		assertEquals(db.getBranchManager().getMasterBranch(), branch.getOrigin());
		assertEquals(tx1.getTimestamp(), branch.getBranchingTimestamp());

		// in the branch, add an additional value to "Two"
		ChronoDBTransaction tx2 = db.tx("MyBranch");
		tx2.put("two", new TestObject("Two", "TO_Two", "Hello World"));
		tx2.commit();

		// now, in the branch, we should be able to find the new entry with a query
		assertEquals(1, db.tx("MyBranch").find().inDefaultKeyspace().where("name").containsIgnoreCase("world").count());
		// we still should find the "one" entry
		assertEquals(1, db.tx("MyBranch").find().inDefaultKeyspace().where("name").containsIgnoreCase("one").count());

		// in the master branch, we know nothing about it
		assertEquals(0, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("world").count());
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
	public void canChangeMultiplicityManyValuesInBranch() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("name", new TestObjectNameIndexer());
		db.getIndexManager().reindexAll();

		ChronoDBTransaction tx1 = db.tx();
		tx1.put("one", new TestObject("One", "TO_One"));
		tx1.put("two", new TestObject("Two", "TO_Two"));
		tx1.commit();

		// assert that we can now find these objects
		assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("One").count());
		assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("TO_One").count());
		assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("Two").count());
		assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("TO_Two").count());

		// create a branch
		Branch branch = db.getBranchManager().createBranch("MyBranch");
		assertNotNull(branch);
		assertEquals(db.getBranchManager().getMasterBranch(), branch.getOrigin());
		assertEquals(tx1.getTimestamp(), branch.getBranchingTimestamp());

		// in the branch, add an additional value to "Two"
		ChronoDBTransaction tx2 = db.tx("MyBranch");
		tx2.put("two", new TestObject("Two", "Hello World"));
		tx2.commit();

		// now, in the branch, we should be able to find the new entry with a query
		assertEquals(1, db.tx("MyBranch").find().inDefaultKeyspace().where("name").containsIgnoreCase("world").count());
		// we shouldn't be able to find it under "TO_Two" in the branch
		assertEquals(0, db.tx("MyBranch").find().inDefaultKeyspace().where("name").isEqualTo("TO_Two").count());

		// in the master branch, we know nothing about the change
		assertEquals(0, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("world").count());
		// ... but we still find the object under its previous name
		assertEquals(1, db.tx().find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("Two").count());
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
	public void canAddNewIndexedValuesInBranch() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("name", new TestObjectNameIndexer());
		db.getIndexManager().reindexAll();

		ChronoDBTransaction tx1 = db.tx();
		tx1.put("one", new TestObject("One"));
		tx1.commit();

		// assert that we can now find the objects
		assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("One").count());

		// create a branch
		Branch branch = db.getBranchManager().createBranch("MyBranch");
		assertNotNull(branch);
		assertEquals(db.getBranchManager().getMasterBranch(), branch.getOrigin());
		assertEquals(tx1.getTimestamp(), branch.getBranchingTimestamp());

		// in the branch, add an additional value to "Two"
		ChronoDBTransaction tx2 = db.tx("MyBranch");
		tx2.put("two", new TestObject("Two"));
		tx2.commit();

		// now, in the branch, we should be able to find the new entry with a query
		assertEquals(1, db.tx("MyBranch").find().inDefaultKeyspace().where("name").containsIgnoreCase("two").count());

		// in the master branch, we know nothing about it
		assertEquals(0, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("two").count());
	}

	private static class TestObject {

		private Set<String> names;

		@SuppressWarnings("unused")
		protected TestObject() {
			// for serialization
		}

		public TestObject(final String... names) {
			this.names = Sets.newHashSet(names);
		}

		public Set<String> getNames() {
			return this.names;
		}

	}

	private static class TestObjectNameIndexer implements ChronoIndexer {

		@Override
		public boolean canIndex(final Object object) {
			return object != null && object instanceof TestObject;
		}

		@Override
		public Set<String> getIndexValues(final Object object) {
			TestObject testObject = (TestObject) object;
			return Sets.newHashSet(testObject.getNames());
		}

	}
}
