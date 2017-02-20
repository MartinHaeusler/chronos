package org.chronos.chronograph.test.commitmetadata;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.chronos.chronodb.api.Order;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

public class GetCommitsBetweenTest extends AllChronoGraphBackendsTest {

	@Test
	public void retrievingInvalidTimeRangeProducesAnEmptyIterator() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit();
		// check
		long now = graph.getNow();
		assertEquals(1, Iterators.size(graph.getCommitTimestampsBetween(0, now)));
		assertEquals(1, Iterators.size(graph.getCommitMetadataBetween(0, now)));
		assertEquals(0, Iterators.size(graph.getCommitTimestampsBetween(now, 0)));
		assertEquals(0, Iterators.size(graph.getCommitMetadataBetween(now, 0)));
	}

	@Test
	public void canRetrieveCommitsWhenNoMetadataWasGiven() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit();
		long now = graph.getNow();
		assertEquals(1, Iterators.size(graph.getCommitTimestampsBetween(0, now)));
		assertEquals(1, Iterators.size(graph.getCommitMetadataBetween(0, now)));
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
				toSet(graph.getCommitTimestampsBetween(commit2, commit4, Order.DESCENDING)));

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true).keySet(),
				// actual
				toSet(graph.getCommitTimestampsBetween(commit2, commit4, Order.ASCENDING)));

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true).descendingMap(),
				// actual
				toMap(graph.getCommitMetadataBetween(commit2, commit4, Order.DESCENDING)));

		assertEquals(
				// expected
				expectedResult.subMap(commit2, true, commit4, true),
				// actual
				toMap(graph.getCommitMetadataBetween(commit2, commit4, Order.ASCENDING)));

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
		assertEquals(2, Iterators.size(graph.getCommitTimestampsBetween(0, commit3)));
		assertEquals(2, Iterators.size(graph.getCommitMetadataBetween(0, commit3)));

		// we should have one commit on 'Test'
		assertEquals(1, Iterators.size(graph.getCommitTimestampsBetween("Test", branchingTimestamp, commit2)));
		assertEquals(1, Iterators.size(graph.getCommitMetadataBetween("Test", branchingTimestamp, commit2)));

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
