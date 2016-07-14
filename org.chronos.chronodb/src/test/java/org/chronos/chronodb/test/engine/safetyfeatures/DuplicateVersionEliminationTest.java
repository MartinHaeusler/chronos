package org.chronos.chronodb.test.engine.safetyfeatures;

import static org.junit.Assert.*;

import java.util.List;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.DuplicateVersionEliminationMode;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(IntegrationTest.class)
public class DuplicateVersionEliminationTest extends AllChronoDBBackendsTest {

	@Test
	public void duplicateVersionEliminationOnCommitWorks() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.txBuilder()
				.withDuplicateVersionEliminationMode(DuplicateVersionEliminationMode.ON_COMMIT).build();
		// add some data
		tx.put("first", 123);
		tx.put("second", 456);
		tx.commit();

		long afterFirstCommit = tx.getTimestamp();

		// try to put a duplicate, an update, and an insert
		tx.put("first", 123); // duplicate
		tx.put("second", "abc"); // update
		tx.put("third", Math.PI); // insert
		tx.commit();

		// assert that the key history of "first" only has one entry.
		// It must have only one, because the second commit on the key was a duplicate and should
		// have been eliminated.
		List<Long> historyTimestamps = Lists.newArrayList(tx.history("first"));
		assertEquals(1, historyTimestamps.size());
		assertTrue(historyTimestamps.contains(afterFirstCommit));

		// assert that the eliminated duplicate is still available in the head revision
		assertEquals(123, (int) tx.get("first"));

	}
}
