package org.chronos.chronodb.test.engine.versioning;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

@Category(IntegrationTest.class)
public class GetModifiedKeysTest extends AllChronoDBBackendsTest {

	@Test
	public void emptyKeyspaceProducesEmptyIterator() {
		ChronoDB db = this.getChronoDB();
		// create some data, just to ensure that the "now" timestamp is non-zero
		ChronoDBTransaction tx1 = db.tx();
		tx1.put("Hello", "World");
		tx1.commit();
		// try to retrieve the modified keys from a non-existing keyspace
		Iterator<TemporalKey> iterator = db.tx().getModificationsInKeyspaceBetween("pseudoKeyspace", 0, db.tx().getTimestamp());
		// the iterator must not be null...
		assertNotNull(iterator);
		// ... but it must be empty
		assertFalse(iterator.hasNext());
	}

	@Test
	public void retrievingSingleCommitAsResultWorks() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx1 = db.tx();
		tx1.put("myKeyspace", "Hello", "World");
		tx1.commit();
		// try to retrieve the timestamp of the commit we just performed
		Iterator<TemporalKey> iterator = db.tx().getModificationsInKeyspaceBetween("myKeyspace", 0, db.tx().getTimestamp());
		assertNotNull(iterator);
		List<TemporalKey> iteratorContents = Lists.newArrayList(iterator);
		assertEquals(1, iteratorContents.size());
		TemporalKey modifiedKey = Iterables.getOnlyElement(iteratorContents);
		assertNotNull(modifiedKey);
		assertEquals(tx1.getTimestamp(), modifiedKey.getTimestamp());
		assertEquals("myKeyspace", modifiedKey.getKeyspace());
		assertEquals("Hello", modifiedKey.getKey());
	}

	@Test
	public void retrievingMultipleCommitsWorks() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx1 = db.tx();
		tx1.put("myKeyspace", "np1", NamedPayload.create1KB("NP1"));
		tx1.put("myKeyspace", "np2", NamedPayload.create1KB("NP2"));
		tx1.put("yourKeyspace", "np1", NamedPayload.create1KB("NP1"));
		tx1.put("yourKeyspace", "np2", NamedPayload.create1KB("NP2"));
		tx1.commit();
		long afterTx1 = tx1.getTimestamp();

		this.sleep(5);

		ChronoDBTransaction tx2 = db.tx();
		tx2.put("myKeyspace", "np1", NamedPayload.create1KB("NP1a"));
		tx2.put("myKeyspace", "np3", NamedPayload.create1KB("NP3"));
		tx2.commit();

		long afterTx2 = tx2.getTimestamp();

		this.sleep(5);

		ChronoDBTransaction tx3 = db.tx();
		tx3.put("myKeyspace", "np1", NamedPayload.create1KB("NP1b"));
		tx3.put("yourKeyspace", "np3", NamedPayload.create1KB("NP3"));
		tx3.commit();

		long afterTx3 = tx3.getTimestamp();

		List<TemporalKey> zeroTo2InMyKeyspace = Lists.newArrayList(db.tx().getModificationsInKeyspaceBetween("myKeyspace", 0, afterTx2));
		assertEquals(4, zeroTo2InMyKeyspace.size());
		assertTrue(zeroTo2InMyKeyspace.contains(TemporalKey.create(afterTx1, "myKeyspace", "np1")));
		assertTrue(zeroTo2InMyKeyspace.contains(TemporalKey.create(afterTx1, "myKeyspace", "np2")));
		assertTrue(zeroTo2InMyKeyspace.contains(TemporalKey.create(afterTx2, "myKeyspace", "np1")));
		assertTrue(zeroTo2InMyKeyspace.contains(TemporalKey.create(afterTx2, "myKeyspace", "np3")));

		List<TemporalKey> zeroTo3InMyKeyspace = Lists.newArrayList(db.tx().getModificationsInKeyspaceBetween("myKeyspace", 0, afterTx3));
		assertEquals(5, zeroTo3InMyKeyspace.size());
		assertTrue(zeroTo3InMyKeyspace.contains(TemporalKey.create(afterTx1, "myKeyspace", "np1")));
		assertTrue(zeroTo3InMyKeyspace.contains(TemporalKey.create(afterTx1, "myKeyspace", "np2")));
		assertTrue(zeroTo3InMyKeyspace.contains(TemporalKey.create(afterTx2, "myKeyspace", "np1")));
		assertTrue(zeroTo3InMyKeyspace.contains(TemporalKey.create(afterTx2, "myKeyspace", "np3")));
		assertTrue(zeroTo3InMyKeyspace.contains(TemporalKey.create(afterTx3, "myKeyspace", "np1")));

		List<TemporalKey> twoTo3InMyKeyspace = Lists.newArrayList(db.tx().getModificationsInKeyspaceBetween("myKeyspace", afterTx2, afterTx3));
		assertEquals(3, twoTo3InMyKeyspace.size());
		assertTrue(twoTo3InMyKeyspace.contains(TemporalKey.create(afterTx2, "myKeyspace", "np1")));
		assertTrue(twoTo3InMyKeyspace.contains(TemporalKey.create(afterTx2, "myKeyspace", "np3")));
		assertTrue(twoTo3InMyKeyspace.contains(TemporalKey.create(afterTx3, "myKeyspace", "np1")));

		List<TemporalKey> exactly3InMyKeyspace = Lists.newArrayList(db.tx().getModificationsInKeyspaceBetween("myKeyspace", afterTx3, afterTx3));
		assertEquals(1, exactly3InMyKeyspace.size());
		assertTrue(exactly3InMyKeyspace.contains(TemporalKey.create(afterTx3, "myKeyspace", "np1")));

	}
}
