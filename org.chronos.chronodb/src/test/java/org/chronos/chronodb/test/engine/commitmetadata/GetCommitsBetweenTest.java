package org.chronos.chronodb.test.engine.commitmetadata;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
import com.google.common.collect.Maps;

public class GetCommitsBetweenTest extends AllChronoDBBackendsTest {

	@Test
	public void retrievingInvalidTimeRangeProducesAnEmptyIterator() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit();
		// check
		long now = tx.getTimestamp();
		assertEquals(1, Iterators.size(tx.getCommitTimestampsBetween(0, now)));
		assertEquals(1, Iterators.size(tx.getCommitMetadataBetween(0, now)));
		assertEquals(0, Iterators.size(tx.getCommitTimestampsBetween(now, 0)));
		assertEquals(0, Iterators.size(tx.getCommitMetadataBetween(now, 0)));
	}

	@Test
	public void canRetrieveCommitsWhenNoMetadataWasGiven() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit();
		long now = tx.getTimestamp();
		assertEquals(1, Iterators.size(tx.getCommitTimestampsBetween(0, now)));
		assertEquals(1, Iterators.size(tx.getCommitMetadataBetween(0, now)));
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
				toSet(tx.getCommitTimestampsBetween(commit2, commit4, Order.DESCENDING)));

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true).keySet(),
				// actual
				toSet(tx.getCommitTimestampsBetween(commit2, commit4, Order.ASCENDING)));

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true).descendingMap(),
				// actual
				toMap(tx.getCommitMetadataBetween(commit2, commit4, Order.DESCENDING)));

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true),
				// actual
				toMap(tx.getCommitMetadataBetween(commit2, commit4, Order.ASCENDING)));

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
		assertEquals(2, Iterators.size(db.tx().getCommitTimestampsBetween(0, commit3)));
		assertEquals(2, Iterators.size(db.tx().getCommitMetadataBetween(0, commit3)));

		// we should have one commit on 'Test'
		assertEquals(1, Iterators.size(db.tx("Test").getCommitTimestampsBetween(branchingTimestamp, commit2)));
		assertEquals(1, Iterators.size(db.tx("Test").getCommitMetadataBetween(branchingTimestamp, commit2)));

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
}
