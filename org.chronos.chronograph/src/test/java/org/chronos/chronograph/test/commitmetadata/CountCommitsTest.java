package org.chronos.chronograph.test.commitmetadata;

import static org.junit.Assert.*;

import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.junit.Test;

public class CountCommitsTest extends AllChronoGraphBackendsTest {

	@Test
	public void retrievingInvalidTimeRangeProducesZero() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit();
		// check
		assertEquals(1, graph.countCommitTimestamps());
		assertEquals(0, graph.countCommitTimestampsBetween(1, 0));
		assertEquals(0, graph.countCommitTimestampsBetween(graph.getNow(), 0));
	}

	@Test
	public void canCountCommitTimestampWhenNoMetadataIsGiven() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit();
		// check
		long now = graph.getNow();
		assertEquals(1, graph.countCommitTimestamps());
		assertEquals(1, graph.countCommitTimestampsBetween(0L, now));
	}

	@Test
	public void canCountCommitsWithAndWithoutMetadata() {
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

		assertEquals(4, graph.countCommitTimestamps());
		assertEquals(2, graph.countCommitTimestampsBetween(commit2, commit3));
		assertEquals(3, graph.countCommitTimestampsBetween(commit1, commit3));
		assertEquals(4, graph.countCommitTimestampsBetween(0, commit4));
	}

	@Test
	public void countingCommitTimestampsIsBranchLocal() {
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
		assertEquals(2, graph.countCommitTimestamps());
		assertEquals(2, graph.countCommitTimestampsBetween(0, commit3));

		// we should have one commit on 'Test'
		assertEquals(1, graph.countCommitTimestamps("Test"));
		assertEquals(1, graph.countCommitTimestampsBetween("Test", branchingTimestamp, commit2));
	}
}
