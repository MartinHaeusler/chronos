package org.chronos.chronodb.test.engine.transaction;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.exceptions.TransactionIsReadOnlyException;
import org.chronos.chronodb.internal.impl.engines.base.ThreadSafeChronoDBTransaction;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TransactionConfigurationTest extends AllChronoDBBackendsTest {

	@Test
	public void threadSafeConfigurationCreatesThreadSafeTransaction() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.txBuilder().threadSafe().build();
		assertTrue(tx instanceof ThreadSafeChronoDBTransaction);
	}

	@Test
	public void configuringBlindOverwriteWorks() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.txBuilder().withBlindOverwriteProtection(true).build();
		assertTrue(tx.getConfiguration().isBlindOverwriteProtectionEnabled());
		ChronoDBTransaction tx2 = db.txBuilder().withBlindOverwriteProtection(false).build();
		assertFalse(tx2.getConfiguration().isBlindOverwriteProtectionEnabled());
	}

	@Test
	public void cantWriteInReadOnlyTransaction() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.txBuilder().readOnly().build();
		try {
			tx.put("a", 123);
			fail();
		} catch (TransactionIsReadOnlyException expected) {
		}
		try {
			tx.remove("a");
			fail();
		} catch (TransactionIsReadOnlyException expected) {
		}
	}

	@Test
	public void branchConfigurationWorks() {
		ChronoDB db = this.getChronoDB();
		db.getBranchManager().createBranch("MyBranch");
		ChronoDBTransaction tx = db.txBuilder().onBranch("MyBranch").build();
		assertEquals("MyBranch", tx.getBranchName());
	}

	@Test
	public void transactionsRunOnMasterBranchByDefault() {
		ChronoDB db = this.getChronoDB();
		db.getBranchManager().createBranch("MyBranch");
		ChronoDBTransaction tx = db.txBuilder().build();
		assertEquals(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, tx.getBranchName());
	}
}
