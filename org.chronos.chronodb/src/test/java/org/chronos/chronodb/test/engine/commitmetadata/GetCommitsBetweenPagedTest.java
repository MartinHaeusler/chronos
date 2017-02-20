package org.chronos.chronodb.test.engine.commitmetadata;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class GetCommitsBetweenPagedTest extends AllChronoDBBackendsTest {

	@Test
	public void retrievingInvalidTimeRangeProducesAnEmptyIterator() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit();
		// check
		long now = tx.getTimestamp();
		assertEquals(1, Iterators.size(tx.getCommitTimestampsPaged(0, now, 1, 0, Order.ASCENDING)));
		assertEquals(1, Iterators.size(tx.getCommitMetadataPaged(0, now, 1, 0, Order.ASCENDING)));
		assertEquals(0, Iterators.size(tx.getCommitTimestampsPaged(now, 0, 1, 0, Order.ASCENDING)));
		assertEquals(0, Iterators.size(tx.getCommitMetadataPaged(now, 0, 1, 0, Order.ASCENDING)));
	}

	@Test
	public void canRetrieveCommitsWhenNoMetadataWasGiven() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit();
		long now = tx.getTimestamp();
		assertEquals(1, Iterators.size(tx.getCommitTimestampsPaged(0, now, 1, 0, Order.ASCENDING)));
		assertEquals(1, Iterators.size(tx.getCommitMetadataPaged(0, now, 1, 0, Order.ASCENDING)));
	}

	@Test
	public void canRetrieveCommitsWithAndWithoutMetadata() {
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

		NavigableMap<Long, Object> expectedResult = Maps.newTreeMap();
		expectedResult.put(commit1, "Initial Commit");
		expectedResult.put(commit2, null);
		expectedResult.put(commit3, "Foobar");
		expectedResult.put(commit4, null);

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true).descendingKeySet(),
				// actual
				toSet(tx.getCommitTimestampsPaged(commit2, commit4, 4, 0, Order.DESCENDING)));

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true).keySet(),
				// actual
				toSet(tx.getCommitTimestampsPaged(commit2, commit4, 4, 0, Order.ASCENDING)));

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true).descendingMap(),
				// actual
				toMap(tx.getCommitMetadataPaged(commit2, commit4, 4, 0, Order.DESCENDING)));

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true),
				// actual
				toMap(tx.getCommitMetadataPaged(commit2, commit4, 4, 0, Order.ASCENDING)));

	}

	@Test
	public void retrievingCommitTimestampsIsBranchLocal() {
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
		assertEquals(2, Iterators.size(db.tx().getCommitTimestampsPaged(0, commit3, 4, 0, Order.ASCENDING)));
		assertEquals(2, Iterators.size(db.tx().getCommitMetadataPaged(0, commit3, 4, 0, Order.ASCENDING)));

		// we should have one commit on 'Test'
		assertEquals(1, Iterators
				.size(db.tx("Test").getCommitTimestampsPaged(branchingTimestamp, commit2, 4, 0, Order.ASCENDING)));
		assertEquals(1, Iterators
				.size(db.tx("Test").getCommitMetadataPaged(branchingTimestamp, commit2, 4, 0, Order.ASCENDING)));

	}

	@Test
	public void ascendingPaginationWorks() {
		ChronoDB db = this.getChronoDB();
		// do 20 commits on the database
		List<Long> commitTimestamps = Lists.newArrayList();
		for (int i = 0; i < 20; i++) {
			ChronoDBTransaction tx = db.tx();
			tx.put("test", i);
			// attach the iteration as metadata to the commit
			tx.commit(i);
			commitTimestamps.add(tx.getTimestamp());
		}

		// query the metadata
		List<Entry<Long, Object>> entriesPage;
		List<Long> timestampsPage;
		// first page
		entriesPage = toList(db.tx().getCommitMetadataPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 0,
				Order.ASCENDING));
		timestampsPage = toList(db.tx().getCommitTimestampsPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5,
				0, Order.ASCENDING));
		assertEquals(5, entriesPage.size());
		for (int i = 0; i < 5; i++) {
			assertEquals(commitTimestamps.get(2 + i), entriesPage.get(i).getKey());
			assertEquals(2 + i, entriesPage.get(i).getValue());
			assertEquals(commitTimestamps.get(2 + i), timestampsPage.get(i));
		}
		// page from the middle
		entriesPage = toList(db.tx().getCommitMetadataPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 2,
				Order.ASCENDING));
		timestampsPage = toList(db.tx().getCommitTimestampsPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5,
				2, Order.ASCENDING));
		assertEquals(5, entriesPage.size());
		for (int i = 0; i < 5; i++) {
			assertEquals(commitTimestamps.get(12 + i), entriesPage.get(i).getKey());
			assertEquals(12 + i, entriesPage.get(i).getValue());
			assertEquals(commitTimestamps.get(12 + i), timestampsPage.get(i));
		}
		// last page (which is incomplete because there are no 5 entries left)
		entriesPage = toList(db.tx().getCommitMetadataPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 3,
				Order.ASCENDING));
		timestampsPage = toList(db.tx().getCommitTimestampsPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5,
				3, Order.ASCENDING));
		assertEquals(2, entriesPage.size());
		for (int i = 0; i < 2; i++) {
			assertEquals(commitTimestamps.get(17 + i), entriesPage.get(i).getKey());
			assertEquals(17 + i, entriesPage.get(i).getValue());
			assertEquals(commitTimestamps.get(17 + i), timestampsPage.get(i));
		}
	}

	@Test
	public void descendingPaginationWorks() {
		ChronoDB db = this.getChronoDB();
		// do 20 commits on the database
		List<Long> commitTimestamps = Lists.newArrayList();
		for (int i = 0; i < 20; i++) {
			ChronoDBTransaction tx = db.tx();
			tx.put("test", i);
			// attach the iteration as metadata to the commit
			tx.commit(i);
			commitTimestamps.add(tx.getTimestamp());
		}

		// query the metadata
		List<Entry<Long, Object>> entriesPage;
		List<Long> timestampsPage;
		// first page
		entriesPage = toList(db.tx().getCommitMetadataPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 0,
				Order.DESCENDING));
		timestampsPage = toList(db.tx().getCommitTimestampsPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5,
				0, Order.DESCENDING));
		assertEquals(5, entriesPage.size());
		for (int i = 0; i < 5; i++) {
			assertEquals(commitTimestamps.get(18 - i), entriesPage.get(i).getKey());
			assertEquals(18 - i, entriesPage.get(i).getValue());
			assertEquals(commitTimestamps.get(18 - i), timestampsPage.get(i));
		}
		// page from the middle
		entriesPage = toList(db.tx().getCommitMetadataPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 2,
				Order.DESCENDING));
		timestampsPage = toList(db.tx().getCommitTimestampsPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5,
				2, Order.DESCENDING));
		assertEquals(5, entriesPage.size());
		for (int i = 0; i < 5; i++) {
			assertEquals(commitTimestamps.get(8 - i), entriesPage.get(i).getKey());
			assertEquals(8 - i, entriesPage.get(i).getValue());
			assertEquals(commitTimestamps.get(8 - i), timestampsPage.get(i));
		}
		// last page (which is incomplete because there are no 5 entries left)
		entriesPage = toList(db.tx().getCommitMetadataPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 3,
				Order.DESCENDING));
		timestampsPage = toList(db.tx().getCommitTimestampsPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5,
				3, Order.DESCENDING));
		assertEquals(2, entriesPage.size());
		for (int i = 0; i < 2; i++) {
			assertEquals(commitTimestamps.get(3 - i), entriesPage.get(i).getKey());
			assertEquals(3 - i, entriesPage.get(i).getValue());
			assertEquals(commitTimestamps.get(3 - i), timestampsPage.get(i));
		}
	}

	// =====================================================================================================================
	// HELPER METHODS
	// =====================================================================================================================

	private static <A, B> Map<A, B> toMap(final Iterator<Entry<A, B>> iterator) {
		Map<A, B> resultMap = new LinkedHashMap<>();
		while (iterator.hasNext()) {
			Entry<A, B> entry = iterator.next();
			resultMap.put(entry.getKey(), entry.getValue());
		}
		return resultMap;
	}

	private static <A> Set<A> toSet(final Iterator<A> iterator) {
		Set<A> resultSet = new LinkedHashSet<>();
		while (iterator.hasNext()) {
			resultSet.add(iterator.next());
		}
		return resultSet;
	}

	private static <A> List<A> toList(final Iterator<A> iterator) {
		List<A> resultList = Lists.newArrayList();
		iterator.forEachRemaining(element -> resultList.add(element));
		return resultList;
	}

}
