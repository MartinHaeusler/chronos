//package org.chronos.chronodb.test.engine.branching;
//
//import org.chronos.chronodb.api.ChronoDB;
//import org.chronos.chronodb.api.ChronoDBConstants;
//import org.chronos.chronodb.api.ChronoDBTransaction;
//import org.chronos.chronodb.api.exceptions.ChronoDBBranchingException;
//import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
//import org.chronos.common.test.junit.categories.IntegrationTest;
//import org.junit.Test;
//import org.junit.experimental.categories.Category;
//
//@Category(IntegrationTest.class)
//public class BranchDeletionTest extends AllChronoDBBackendsTest {
//
//	@Test
//	public void cannotRemoveMasterBranch() {
//		ChronoDB db = this.getChronoDB();
//		try {
//			db.getBranchManager().removeBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
//			fail("Managed to delete master branch!");
//		} catch (ChronoDBBranchingException e) {
//			// expected
//		}
//		// assert that the master branch is still present
//		assertTrue(db.getBranchManager().existsBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER));
//	}
//
//	@Test
//	public void removingABranchWorks() {
//		ChronoDB db = this.getChronoDB();
//		// assert that the branch initially does not exist
//		assertEquals(false, db.getBranchManager().existsBranch("MyBranch"));
//		// create the branch
//		db.getBranchManager().createBranch("MyBranch");
//		// assert that it exists
//		db.getBranchManager().existsBranch("MyBranch");
//		// fill it with some data
//		ChronoDBTransaction tx = db.tx("MyBranch");
//		tx.put("first", 123);
//		tx.put("second", 456);
//		tx.commit();
//		tx.put("third", 789);
//		tx.commit();
//		// assert that the data is present in the branch
//		assertEquals(3, tx.keySet().size());
//		// assert that the master branch is still empty
//		tx = db.tx();
//		assertTrue(tx.keySet().isEmpty());
//		// remove the branch
//		db.getBranchManager().removeBranch("MyBranch");
//		// assert that the branch is gone
//		assertEquals(false, db.getBranchManager().existsBranch("MyBranch"));
//	}
//}
