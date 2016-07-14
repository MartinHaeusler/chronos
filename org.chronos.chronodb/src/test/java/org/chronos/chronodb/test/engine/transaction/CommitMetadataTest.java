package org.chronos.chronodb.test.engine.transaction;

import static org.junit.Assert.*;

import java.util.Map;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;

@Category(IntegrationTest.class)
public class CommitMetadataTest extends AllChronoDBBackendsTest {

	@Test
	public void simpleStoringAndRetrievingMetadataWorks() {
		ChronoDB db = this.getChronoDB();
		// just a message that we can compare later on
		String commitMessage = "Hey there, Hello World!";
		// insert data into the database
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit(commitMessage);
		// record the commit timestamp
		long timestamp = tx.getTimestamp();
		// read the metadata and assert that it's correct
		assertEquals(commitMessage, db.tx().getCommitMetadata(timestamp));
	}

	@Test
	public void readingCommitMetadataFromNonExistingCommitTimestampsProducesNull() {
		ChronoDB db = this.getChronoDB();
		// insert data into the database
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("Hello World!");
		// read commit metadata from a non-existing (but valid) commit timestamp
		assertNull(db.tx().getCommitMetadata(0L));
	}

	@Test
	public void readingCommitMetadataRetrievesCorrectObject() {
		ChronoDB db = this.getChronoDB();
		// the commit messages, as constants to compare with later on
		final String commitMessage1 = "Hello World!";
		final String commitMessage2 = "Foo Bar";
		final String commitMessage3 = "Chronos is awesome";
		// insert data into the database
		ChronoDBTransaction tx1 = db.tx();
		tx1.put("Hello", "World");
		tx1.commit(commitMessage1);
		// record the commit timestamp
		long timeCommit1 = tx1.getTimestamp();

		this.sleep(5);

		// ... and another record
		ChronoDBTransaction tx2 = db.tx();
		tx2.put("Foo", "Bar");
		tx2.commit(commitMessage2);
		// record the commit timestamp
		long timeCommit2 = tx2.getTimestamp();

		this.sleep(5);

		// ... and yet another record
		ChronoDBTransaction tx3 = db.tx();
		tx3.put("Chronos", "IsAwesome");
		tx3.commit(commitMessage3);
		// record the commit timestamp
		long timeCommit3 = tx3.getTimestamp();

		this.sleep(5);

		// now, retrieve the three messages individually
		assertEquals(commitMessage1, db.tx().getCommitMetadata(timeCommit1));
		assertEquals(commitMessage2, db.tx().getCommitMetadata(timeCommit2));
		assertEquals(commitMessage3, db.tx().getCommitMetadata(timeCommit3));
	}

	@Test
	public void readingCommitMessagesFromOriginBranchWorks() {
		ChronoDB db = this.getChronoDB();
		// the commit messages, as constants to compare with later on
		final String commitMessage1 = "Hello World!";
		final String commitMessage2 = "Foo Bar";
		// insert data into the database
		ChronoDBTransaction tx1 = db.tx();
		tx1.put("Hello", "World");
		tx1.commit(commitMessage1);
		// record the commit timestamp
		long timeCommit1 = tx1.getTimestamp();

		this.sleep(5);

		// create the branch
		db.getBranchManager().createBranch("MyBranch");

		// commit something to the branch
		ChronoDBTransaction tx2 = db.tx("MyBranch");
		tx2.put("Foo", "Bar");
		tx2.commit(commitMessage2);
		// record the commit timestamp
		long timeCommit2 = tx2.getTimestamp();

		// open a transaction on the branch and request the commit message of tx1
		assertEquals(commitMessage1, db.tx("MyBranch").getCommitMetadata(timeCommit1));
		// also, we should be able to get the commit metadata of the branch
		assertEquals(commitMessage2, db.tx("MyBranch").getCommitMetadata(timeCommit2));
	}

	@Test
	public void canUseHashMapAsCommitMetadata() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		Map<String, String> commitMap = Maps.newHashMap();
		commitMap.put("User", "Martin");
		commitMap.put("Mood", "Good");
		commitMap.put("Mail", "martin.haeusler@uibk.ac.at");
		tx.commit(commitMap);
		long timestamp = tx.getTimestamp();

		// get the commit map
		@SuppressWarnings("unchecked")
		Map<String, String> retrieved = (Map<String, String>) db.tx().getCommitMetadata(timestamp);
		// assert that the maps are equal
		assertNotNull(retrieved);
		assertEquals(commitMap, retrieved);
	}
}
