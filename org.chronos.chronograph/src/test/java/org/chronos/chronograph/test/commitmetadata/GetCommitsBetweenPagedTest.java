package org.chronos.chronograph.test.commitmetadata;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.chronos.chronodb.api.Order;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class GetCommitsBetweenPagedTest extends AllChronoGraphBackendsTest {

	@Test
	public void retrievingInvalidTimeRangeProducesAnEmptyIterator() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit();
		// check
		long now = graph.getNow();
		assertEquals(1, Iterators.size(graph.getCommitTimestampsPaged(0, now, 1, 0, Order.ASCENDING)));
		assertEquals(1, Iterators.size(graph.getCommitMetadataPaged(0, now, 1, 0, Order.ASCENDING)));
		assertEquals(0, Iterators.size(graph.getCommitTimestampsPaged(now, 0, 1, 0, Order.ASCENDING)));
		assertEquals(0, Iterators.size(graph.getCommitMetadataPaged(now, 0, 1, 0, Order.ASCENDING)));
	}

	@Test
	public void canRetrieveCommitsWhenNoMetadataWasGiven() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit();
		long now = graph.getNow();
		assertEquals(1, Iterators.size(graph.getCommitTimestampsPaged(0, now, 1, 0, Order.ASCENDING)));
		assertEquals(1, Iterators.size(graph.getCommitMetadataPaged(0, now, 1, 0, Order.ASCENDING)));
	}

	@Test
	public void canRetrieveCommitsWithAndWithoutMetadata() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("Initial Commit");
		final long commit1 = graph.getNow();

		graph.addVertex("Hello", "Test");
		graph.tx().commit();
		final long commit2 = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Foobar");
		final long commit3 = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit();
		final long commit4 = graph.getNow();

		NavigableMap<Long, Object> expectedResult = Maps.newTreeMap();
		expectedResult.put(commit1, "Initial Commit");
		expectedResult.put(commit2, null);
		expectedResult.put(commit3, "Foobar");
		expectedResult.put(commit4, null);

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true).descendingKeySet(),
				// actual
				toSet(graph.getCommitTimestampsPaged(commit2, commit4, 4, 0, Order.DESCENDING)));

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true).keySet(),
				// actual
				toSet(graph.getCommitTimestampsPaged(commit2, commit4, 4, 0, Order.ASCENDING)));

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true).descendingMap(),
				// actual
				toMap(graph.getCommitMetadataPaged(commit2, commit4, 4, 0, Order.DESCENDING)));

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true),
				// actual
				toMap(graph.getCommitMetadataPaged(commit2, commit4, 4, 0, Order.ASCENDING)));

	}

	@Test
	public void retrievingCommitTimestampsIsBranchLocal() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("Initial Commit");

		graph.getBranchManager().createBranch("Test");
		long branchingTimestamp = graph.getBranchManager().getBranch("Test").getBranchingTimestamp();

		ChronoGraph txGraph = graph.tx().createThreadedTx("Test");
		try {
			txGraph.addVertex("Foo", "Bar");
			txGraph.tx().commit("New Branch!");
		} finally {
			txGraph.close();
		}
		final long commit2 = graph.getNow("Test");

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit();
		final long commit3 = graph.getNow();

		// we should have two commits on 'master' (the first one and the last one)
		assertEquals(2, Iterators.size(graph.getCommitTimestampsPaged(0, commit3, 4, 0, Order.ASCENDING)));
		assertEquals(2, Iterators.size(graph.getCommitMetadataPaged(0, commit3, 4, 0, Order.ASCENDING)));

		// we should have one commit on 'Test'
		assertEquals(1, Iterators
				.size(graph.getCommitTimestampsPaged("Test", branchingTimestamp, commit2, 4, 0, Order.ASCENDING)));
		assertEquals(1, Iterators
				.size(graph.getCommitMetadataPaged("Test", branchingTimestamp, commit2, 4, 0, Order.ASCENDING)));

	}

	@Test
	public void ascendingPaginationWorks() {
		ChronoGraph graph = this.getGraph();
		// do 20 commits on the database
		List<Long> commitTimestamps = Lists.newArrayList();
		for (int i = 0; i < 20; i++) {
			graph.addVertex("test", i);
			// attach the iteration as metadata to the commit
			graph.tx().commit(i);
			commitTimestamps.add(graph.getNow());
		}

		// query the metadata
		List<Entry<Long, Object>> entriesPage;
		List<Long> timestampsPage;
		// first page
		entriesPage = toList(
				graph.getCommitMetadataPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 0, Order.ASCENDING));
		timestampsPage = toList(graph.getCommitTimestampsPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 0,
				Order.ASCENDING));
		assertEquals(5, entriesPage.size());
		for (int i = 0; i < 5; i++) {
			assertEquals(commitTimestamps.get(2 + i), entriesPage.get(i).getKey());
			assertEquals(2 + i, entriesPage.get(i).getValue());
			assertEquals(commitTimestamps.get(2 + i), timestampsPage.get(i));
		}
		// page from the middle
		entriesPage = toList(
				graph.getCommitMetadataPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 2, Order.ASCENDING));
		timestampsPage = toList(graph.getCommitTimestampsPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 2,
				Order.ASCENDING));
		assertEquals(5, entriesPage.size());
		for (int i = 0; i < 5; i++) {
			assertEquals(commitTimestamps.get(12 + i), entriesPage.get(i).getKey());
			assertEquals(12 + i, entriesPage.get(i).getValue());
			assertEquals(commitTimestamps.get(12 + i), timestampsPage.get(i));
		}
		// last page (which is incomplete because there are no 5 entries left)
		entriesPage = toList(
				graph.getCommitMetadataPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 3, Order.ASCENDING));
		timestampsPage = toList(graph.getCommitTimestampsPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 3,
				Order.ASCENDING));
		assertEquals(2, entriesPage.size());
		for (int i = 0; i < 2; i++) {
			assertEquals(commitTimestamps.get(17 + i), entriesPage.get(i).getKey());
			assertEquals(17 + i, entriesPage.get(i).getValue());
			assertEquals(commitTimestamps.get(17 + i), timestampsPage.get(i));
		}
	}

	@Test
	public void descendingPaginationWorks() {
		ChronoGraph graph = this.getGraph();
		// do 20 commits on the database
		List<Long> commitTimestamps = Lists.newArrayList();
		for (int i = 0; i < 20; i++) {
			graph.addVertex("test", i);
			// attach the iteration as metadata to the commit
			graph.tx().commit(i);
			commitTimestamps.add(graph.getNow());
		}

		// query the metadata
		List<Entry<Long, Object>> entriesPage;
		List<Long> timestampsPage;
		// first page
		entriesPage = toList(graph.getCommitMetadataPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 0,
				Order.DESCENDING));
		timestampsPage = toList(graph.getCommitTimestampsPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 0,
				Order.DESCENDING));
		assertEquals(5, entriesPage.size());
		for (int i = 0; i < 5; i++) {
			assertEquals(commitTimestamps.get(18 - i), entriesPage.get(i).getKey());
			assertEquals(18 - i, entriesPage.get(i).getValue());
			assertEquals(commitTimestamps.get(18 - i), timestampsPage.get(i));
		}
		// page from the middle
		entriesPage = toList(graph.getCommitMetadataPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 2,
				Order.DESCENDING));
		timestampsPage = toList(graph.getCommitTimestampsPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 2,
				Order.DESCENDING));
		assertEquals(5, entriesPage.size());
		for (int i = 0; i < 5; i++) {
			assertEquals(commitTimestamps.get(8 - i), entriesPage.get(i).getKey());
			assertEquals(8 - i, entriesPage.get(i).getValue());
			assertEquals(commitTimestamps.get(8 - i), timestampsPage.get(i));
		}
		// last page (which is incomplete because there are no 5 entries left)
		entriesPage = toList(graph.getCommitMetadataPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 3,
				Order.DESCENDING));
		timestampsPage = toList(graph.getCommitTimestampsPaged(commitTimestamps.get(2), commitTimestamps.get(18), 5, 3,
				Order.DESCENDING));
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
