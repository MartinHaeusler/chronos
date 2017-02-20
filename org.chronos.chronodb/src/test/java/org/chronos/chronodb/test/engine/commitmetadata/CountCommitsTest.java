package org.chronos.chronodb.test.engine.commitmetadata;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.junit.Test;

public class CountCommitsTest extends AllChronoDBBackendsTest {

	@Test
	public void retrievingInvalidTimeRangeProducesZero() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit();
		// check
		assertEquals(1, tx.countCommitTimestamps());
		assertEquals(0, tx.countCommitTimestampsBetween(1, 0));
		assertEquals(0, tx.countCommitTimestampsBetween(tx.getTimestamp(), 0));
	}

	@Test
	public void canCountCommitTimestampWhenNoMetadataIsGiven() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit();
		// check
		long now = tx.getTimestamp();
		assertEquals(1, tx.countCommitTimestamps());
		assertEquals(1, tx.countCommitTimestampsBetween(0L, now));
	}

	@Test
	public void canCountCommitsWithAndWithoutMetadata() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("Initial Commit");
		final long commit1 = tx.getTimestamp();

		tx.put("Hello", "Test");
		tx.commit();
		final long commit2 = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Foobar");
		final long commit3 = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit();
		final long commit4 = tx.getTimestamp();

		assertEquals(4, tx.countCommitTimestamps());
		assertEquals(2, tx.countCommitTimestampsBetween(commit2, commit3));
		assertEquals(3, tx.countCommitTimestampsBetween(commit1, commit3));
		assertEquals(4, tx.countCommitTimestampsBetween(0, commit4));
	}

	@Test
	public void countingCommitTimestampsIsBranchLocal() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("Initial Commit");

		db.getBranchManager().createBranch("Test");
		long branchingTimestamp = db.getBranchManager().getBranch("Test").getBranchingTimestamp();

		tx = db.tx("Test");
		tx.put("Foo", "Bar");
		tx.commit("New Branch!");
		final long commit2 = tx.getTimestamp();

		tx = db.tx();
		tx.put("Pi", "3.1415");
		tx.commit();
		final long commit3 = tx.getTimestamp();

		// we should have two commits on 'master' (the first one and the last one)
		assertEquals(2, db.tx().countCommitTimestamps());
		assertEquals(2, db.tx().countCommitTimestampsBetween(0, commit3));

		// we should have one commit on 'Test'
		assertEquals(1, db.tx("Test").countCommitTimestamps());
		assertEquals(1, db.tx("Test").countCommitTimestampsBetween(branchingTimestamp, commit2));
	}
}
