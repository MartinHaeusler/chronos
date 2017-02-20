package org.chronos.chronodb.test.engine.branching;

import static org.junit.Assert.*;

import java.util.Set;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class BranchCreationTest extends AllChronoDBBackendsTest {

	@Test
	public void basicBranchCreationWorks() {
		ChronoDB db = this.getChronoDB();

		// fill the database with some data
		ChronoDBTransaction tx = db.tx();
		tx.put("first", 123);
		tx.put("second", 456);
		tx.commit();

		tx.put("first", 789);
		tx.put("third", 321);
		tx.commit();

		// at this point, our branch should not exist yet
		assertEquals(false, db.getBranchManager().existsBranch("MyBranch"));

		// create the branch
		db.getBranchManager().createBranch("MyBranch");
		// make sure that the branch exists now
		assertTrue(db.getBranchManager().existsBranch("MyBranch"));

		// access the branch
		tx = db.tx("MyBranch");

		// make sure that the tx has the proper timestamp
		assertTrue(tx.getTimestamp() > 0);

		// assert that the data is in the branch
		assertEquals(3, tx.keySet().size());
		assertEquals(789, (int) tx.get("first"));
		assertEquals(456, (int) tx.get("second"));
		assertEquals(321, (int) tx.get("third"));
	}

	@Test
	public void branchingAKeyspaceWorks() {
		ChronoDB db = this.getChronoDB();

		// fill the database with some data
		ChronoDBTransaction tx = db.tx();
		tx.put("first", 123);
		tx.put("MyKeyspace", "second", 456);
		tx.commit();

		tx.put("first", 789);
		tx.put("MyOtherKeyspace", "third", 321);
		tx.commit();

		// at this point, our branch should not exist yet
		assertEquals(false, db.getBranchManager().existsBranch("MyBranch"));

		// create the branch
		db.getBranchManager().createBranch("MyBranch");
		// make sure that the branch exists now
		assertTrue(db.getBranchManager().existsBranch("MyBranch"));

		// access the branch
		tx = db.tx("MyBranch");

		// make sure that the tx has the proper timestamp
		assertTrue(tx.getTimestamp() > 0);

		// assert that the data is in the branch
		assertEquals(1, tx.keySet().size());
		assertEquals(789, (int) tx.get("first"));
		assertEquals(456, (int) tx.get("MyKeyspace", "second"));
		assertEquals(321, (int) tx.get("MyOtherKeyspace", "third"));
	}

	@Test
	public void branchingFromBranchWorks() {
		ChronoDB db = this.getChronoDB();

		// fill the database with some data
		ChronoDBTransaction tx = db.tx();
		tx.put("first", 123);
		tx.put("MyKeyspace", "second", 456);
		tx.commit();

		// create a branch
		db.getBranchManager().createBranch("MyBranch");

		// insert some data into the branch
		tx = db.tx("MyBranch");
		tx.put("Math", "Pi", 31415);
		tx.commit();

		// create a sub-branch
		db.getBranchManager().createBranch("MyBranch", "MySubBranch");

		// commit something in the master branch
		tx = db.tx();
		tx.put("MyAwesomeKeyspace", "third", 789);
		tx.commit();

		// assert that each branch contains the correct information
		tx = db.tx("MySubBranch");
		// Keyspaces in 'MySubBranch': default, MyKeyspace, Math
		assertEquals(3, tx.keyspaces().size());
		assertEquals(31415, (int) tx.get("Math", "Pi"));
		assertEquals(456, (int) tx.get("MyKeyspace", "second"));
		assertEquals(123, (int) tx.get("first"));
		assertEquals(false, tx.keyspaces().contains("MyAwesomeKeyspace"));

		tx = db.tx();
		// Keyspaces in 'Master': default, MyKeyspace, MyAwesomeKeyspace
		assertEquals(3, tx.keyspaces().size());
		assertEquals(789, (int) tx.get("MyAwesomeKeyspace", "third"));

	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
	public void overridingValuesInBranchWorks() {
		ChronoDB db = this.getChronoDB();

		// create three entries in the master branch
		ChronoDBTransaction tx = db.tx();
		tx.put("np1", NamedPayload.create10KB("NP1"));
		tx.put("np2", NamedPayload.create10KB("NP2"));
		tx.put("np3", NamedPayload.create10KB("NP3"));
		tx.commit();

		// remember the timestamp after writing to master
		long afterWritingToMaster = tx.getTimestamp();

		// then, branch from master
		Branch sub = db.getBranchManager().createBranch("Sub");
		assertNotNull(sub);
		assertEquals(db.getBranchManager().getMasterBranch(), sub.getOrigin());
		assertEquals(afterWritingToMaster, sub.getBranchingTimestamp());

		// in the new branch, override the value for "np2"
		ChronoDBTransaction txSub = db.tx("Sub");
		txSub.put("np2", NamedPayload.create10KB("NP2_SUB"));
		txSub.commit();

		// remember the timestamp after writing to sub
		long afterWritingToSub = txSub.getTimestamp();

		// create another branch from sub
		Branch subsub = db.getBranchManager().createBranch("Sub", "SubSub");
		assertNotNull(subsub);
		assertEquals(sub, subsub.getOrigin());
		assertEquals(afterWritingToSub, subsub.getBranchingTimestamp());

		// override the value for "np3" in subsub
		ChronoDBTransaction txSubSub = db.tx("SubSub");
		txSubSub.put("np3", NamedPayload.create10KB("NP3_SUBSUB"));
		txSubSub.commit();

		// in the master branch, we still want to see our original values
		assertEquals("NP1", ((NamedPayload) db.tx().get("np1")).getName());
		assertEquals("NP2", ((NamedPayload) db.tx().get("np2")).getName());
		assertEquals("NP3", ((NamedPayload) db.tx().get("np3")).getName());

		// in the sub branch, we want to see our override of np2
		assertEquals("NP1", ((NamedPayload) db.tx("Sub").get("np1")).getName());
		assertEquals("NP2_SUB", ((NamedPayload) db.tx("Sub").get("np2")).getName());
		assertEquals("NP3", ((NamedPayload) db.tx("Sub").get("np3")).getName());

		// in the subsub branch, we want to see all of our overrides
		assertEquals("NP1", ((NamedPayload) db.tx("SubSub").get("np1")).getName());
		assertEquals("NP2_SUB", ((NamedPayload) db.tx("SubSub").get("np2")).getName());
		assertEquals("NP3_SUBSUB", ((NamedPayload) db.tx("SubSub").get("np3")).getName());

		// on ANY branch, if we request the timestamp after committing to master, we want to see our original values
		assertEquals("NP1", ((NamedPayload) db.tx(afterWritingToMaster).get("np1")).getName());
		assertEquals("NP2", ((NamedPayload) db.tx(afterWritingToMaster).get("np2")).getName());
		assertEquals("NP3", ((NamedPayload) db.tx(afterWritingToMaster).get("np3")).getName());
		assertEquals("NP1", ((NamedPayload) db.tx("Sub", afterWritingToMaster).get("np1")).getName());
		assertEquals("NP2", ((NamedPayload) db.tx("Sub", afterWritingToMaster).get("np2")).getName());
		assertEquals("NP3", ((NamedPayload) db.tx("Sub", afterWritingToMaster).get("np3")).getName());
		assertEquals("NP1", ((NamedPayload) db.tx("SubSub", afterWritingToMaster).get("np1")).getName());
		assertEquals("NP2", ((NamedPayload) db.tx("SubSub", afterWritingToMaster).get("np2")).getName());
		assertEquals("NP3", ((NamedPayload) db.tx("SubSub", afterWritingToMaster).get("np3")).getName());

		// assert that our keyset is the same in all branches, at the timestamp after inserting to master
		Set<String> keySet = Sets.newHashSet("np1", "np2", "np3");
		assertEquals(keySet, db.tx(afterWritingToMaster).keySet());
		assertEquals(keySet, db.tx("Sub", afterWritingToMaster).keySet());
		assertEquals(keySet, db.tx("SubSub", afterWritingToMaster).keySet());

		// in order to assert that we have valid timestamps after our insertions, we insert another pair everywhere
		ChronoDBTransaction tx2 = db.tx();
		tx2.put("np4", NamedPayload.create10KB("NP4"));
		tx2.commit();
		ChronoDBTransaction tx3 = db.tx("Sub");
		tx3.put("np4", NamedPayload.create10KB("NP4"));
		tx3.commit();
		ChronoDBTransaction tx4 = db.tx("SubSub");
		tx4.put("np4", NamedPayload.create10KB("NP4"));
		tx4.commit();

		// after overriding the entry in "Sub", we want to see the original values in master
		assertEquals("NP1", ((NamedPayload) db.tx(afterWritingToSub).get("np1")).getName());
		assertEquals("NP2", ((NamedPayload) db.tx(afterWritingToSub).get("np2")).getName());
		assertEquals("NP3", ((NamedPayload) db.tx(afterWritingToSub).get("np3")).getName());
		// ... but we want to see our changes in sub
		assertEquals("NP1", ((NamedPayload) db.tx("Sub", afterWritingToSub).get("np1")).getName());
		assertEquals("NP2_SUB", ((NamedPayload) db.tx("Sub", afterWritingToSub).get("np2")).getName());
		assertEquals("NP3", ((NamedPayload) db.tx("Sub", afterWritingToSub).get("np3")).getName());
		// ... in SubSub, we don't want to see our change, because it hasn't happened yet
		assertEquals("NP1", ((NamedPayload) db.tx("SubSub", afterWritingToSub).get("np1")).getName());
		assertEquals("NP2_SUB", ((NamedPayload) db.tx("SubSub", afterWritingToSub).get("np2")).getName());
		assertEquals("NP3", ((NamedPayload) db.tx("SubSub", afterWritingToSub).get("np3")).getName());

	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
	public void addingValuesInBranchesWorks() {
		ChronoDB db = this.getChronoDB();

		// create three entries in the master branch
		ChronoDBTransaction tx = db.tx();
		tx.put("np1", NamedPayload.create10KB("NP1"));
		tx.commit();

		// remember the timestamp after writing to master
		long afterWritingToMaster = tx.getTimestamp();

		// then, branch from master
		Branch sub = db.getBranchManager().createBranch("Sub");
		assertNotNull(sub);
		assertEquals(db.getBranchManager().getMasterBranch(), sub.getOrigin());
		assertEquals(afterWritingToMaster, sub.getBranchingTimestamp());

		// in the new branch, override the value for "np2"
		ChronoDBTransaction txSub = db.tx("Sub");
		txSub.put("np2", NamedPayload.create10KB("NP2_SUB"));
		txSub.commit();

		// remember the timestamp after writing to sub
		long afterWritingToSub = txSub.getTimestamp();

		// create another branch from sub
		Branch subsub = db.getBranchManager().createBranch("Sub", "SubSub");
		assertNotNull(subsub);
		assertEquals(sub, subsub.getOrigin());
		assertEquals(afterWritingToSub, subsub.getBranchingTimestamp());

		// override the value for "np3" in subsub
		ChronoDBTransaction txSubSub = db.tx("SubSub");
		txSubSub.put("np3", NamedPayload.create10KB("NP3_SUBSUB"));
		txSubSub.commit();

		// in the master branch, we still want to see our original value, and nothing else
		assertEquals("NP1", ((NamedPayload) db.tx().get("np1")).getName());
		assertNull(db.tx().get("np2"));
		assertNull(db.tx().get("np3"));

		// in the sub branch, we want to see our addition of np2
		assertEquals("NP1", ((NamedPayload) db.tx("Sub").get("np1")).getName());
		assertEquals("NP2_SUB", ((NamedPayload) db.tx("Sub").get("np2")).getName());
		assertNull(db.tx("Sub").get("np3"));

		// in the subsub branch, we want to see all of our additions
		assertEquals("NP1", ((NamedPayload) db.tx("SubSub").get("np1")).getName());
		assertEquals("NP2_SUB", ((NamedPayload) db.tx("SubSub").get("np2")).getName());
		assertEquals("NP3_SUBSUB", ((NamedPayload) db.tx("SubSub").get("np3")).getName());

		// on ANY branch, if we request the timestamp after committing to master, we want to see our original values
		assertEquals("NP1", ((NamedPayload) db.tx(afterWritingToMaster).get("np1")).getName());
		assertNull(db.tx().get("np2"));
		assertNull(db.tx().get("np3"));
		assertEquals("NP1", ((NamedPayload) db.tx("Sub", afterWritingToMaster).get("np1")).getName());
		assertNull(db.tx("Sub", afterWritingToMaster).get("np2"));
		assertNull(db.tx("Sub", afterWritingToMaster).get("np3"));
		assertEquals("NP1", ((NamedPayload) db.tx("SubSub", afterWritingToMaster).get("np1")).getName());
		assertNull(db.tx("SubSub", afterWritingToMaster).get("np2"));
		assertNull(db.tx("SubSub", afterWritingToMaster).get("np3"));

		// assert that the key sets are valid
		assertEquals(Sets.newHashSet("np1"), db.tx().keySet());
		assertEquals(Sets.newHashSet("np1", "np2"), db.tx("Sub").keySet());
		assertEquals(Sets.newHashSet("np1", "np2", "np3"), db.tx("SubSub").keySet());
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
	public void removingValuesInBranchWorks() {
		ChronoDB db = this.getChronoDB();
		// create three entries in the master branch
		ChronoDBTransaction tx = db.tx();
		tx.put("np1", NamedPayload.create10KB("NP1"));
		tx.put("np2", NamedPayload.create10KB("NP2"));
		tx.put("np3", NamedPayload.create10KB("NP3"));
		tx.commit();

		// remember the timestamp for later use
		long afterWritingToMaster = tx.getTimestamp();

		// create a branch
		Branch sub = db.getBranchManager().createBranch("Sub");
		assertNotNull(sub);
		assertEquals(db.getBranchManager().getMasterBranch(), sub.getOrigin());
		assertEquals(afterWritingToMaster, sub.getBranchingTimestamp());

		// remove a value from the branch
		ChronoDBTransaction txSub = db.tx("Sub");
		txSub.remove("np2");
		txSub.commit();

		long afterWritingToSub = txSub.getTimestamp();

		// create another sub-branch
		Branch subSub = db.getBranchManager().createBranch("Sub", "SubSub");
		assertNotNull(subSub);
		assertEquals(sub, subSub.getOrigin());
		assertEquals(afterWritingToSub, subSub.getBranchingTimestamp());

		// remove another value in subsub-branch
		ChronoDBTransaction txSubSub = db.tx("SubSub");
		txSubSub.remove("np3");
		txSubSub.commit();

		// in order to assert that we have valid timestamps after our insertions, we insert another pair everywhere
		ChronoDBTransaction tx2 = db.tx();
		tx2.put("np4", NamedPayload.create10KB("NP4"));
		tx2.commit();
		ChronoDBTransaction tx3 = db.tx("Sub");
		tx3.put("np4", NamedPayload.create10KB("NP4"));
		tx3.commit();
		ChronoDBTransaction tx4 = db.tx("SubSub");
		tx4.put("np4", NamedPayload.create10KB("NP4"));
		tx4.commit();

		// assert that the values are still present in the master branch
		assertNotNull(db.tx().get("np1"));
		assertNotNull(db.tx().get("np2"));
		assertNotNull(db.tx().get("np3"));
		assertNotNull(db.tx().get("np4"));
		assertEquals(Sets.newHashSet("np1", "np2", "np3", "np4"), db.tx().keySet());
		// assert that the "np2" value is gone in Sub, but the other values are still present
		assertNotNull(db.tx("Sub").get("np1"));
		assertNull(db.tx("Sub").get("np2"));
		assertNotNull(db.tx("Sub").get("np3"));
		assertNotNull(db.tx("Sub").get("np4"));
		assertEquals(Sets.newHashSet("np1", "np3", "np4"), db.tx("Sub").keySet());
		// assert that "np2" and "np3" are gone in SubSub, but the other values are still present
		assertNotNull(db.tx("SubSub").get("np1"));
		assertNull(db.tx("SubSub").get("np2"));
		assertNull(db.tx("SubSub").get("np3"));
		assertNotNull(db.tx("SubSub").get("np4"));
		assertEquals(Sets.newHashSet("np1", "np4"), db.tx("SubSub").keySet());
	}
}
