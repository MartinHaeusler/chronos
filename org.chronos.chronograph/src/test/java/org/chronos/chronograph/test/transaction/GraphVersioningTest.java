package org.chronos.chronograph.test.transaction;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class GraphVersioningTest extends AllChronoGraphBackendsTest {

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void graphVersionReadsWork() {
		ChronoGraph g = this.getGraph();
		// remember the timestamp for later reference
		long timestampBeforeTx1 = System.currentTimeMillis();
		// make sure that the timestamp really is BEFORE the insert by sleeping a bit
		this.sleep(1);
		// create a graph:
		//
		// (martin)-[worksOn]->(project1)
		//         -[worksOn]->(project2)
		//         -[knows]->(John)
		// (john)-[worksOn]->(project3)
		Vertex vMartin = g.addVertex("name", "Martin", "kind", "person");
		Vertex vJohn = g.addVertex("name", "John", "kind", "person");
		Vertex vProject1 = g.addVertex("name", "Project 1", "kind", "project");
		Vertex vProject2 = g.addVertex("name", "Project 2", "kind", "project");
		Vertex vProject3 = g.addVertex("name", "Project 3", "kind", "project");
		vMartin.addEdge("knows", vJohn);
		vMartin.addEdge("worksOn", vProject1);
		vMartin.addEdge("worksOn", vProject2);
		vJohn.addEdge("worksOn", vProject3);
		String vMartinId = (String) vMartin.id();
		String vJohnId = (String) vJohn.id();
		String vProject1Id = (String) vProject1.id();
		String vProject2Id = (String) vProject2.id();
		String vProject3Id = (String) vProject3.id();
		g.tx().commit();
		// remember the timestamp for later reference
		long timestampBeforeTx2 = System.currentTimeMillis();
		this.sleep(1);
		// modify the graph to match:
		//
		// (project2) -> delete
		// (martin)-[worksOn]->(project1)
		//         -[worksOn]->(project3)
		//         -[worksOn]->(project4)
		//         -[knows]->(John)
		// (john)-[worksOn]->(project3)
		vProject2.remove();
		Vertex vProject4 = g.addVertex("name", "Project 4", "kind", "project");
		vMartin.addEdge("worksOn", vProject3);
		vMartin.addEdge("worksOn", vProject4);
		String vProject4Id = (String) vProject4.id();
		g.tx().commit();
		// remember the timestamp for later reference
		long timestampBeforeTx3 = System.currentTimeMillis();
		this.sleep(1);
		// modify the graph to match:
		//
		// (martin)-[worksOn]->(project3)
		//         -[worksOn]->(project4)
		//         -[knows]->(John)
		// (john)-[worksOn]->(project3)
		//       -[worksOn]->(project1)
		Edge eMartinWorksOnProject1 = Iterators.getOnlyElement(Iterators.filter(vMartin.edges(Direction.OUT, "worksOn"), e -> e.inVertex().equals(vProject1)));
		eMartinWorksOnProject1.remove();
		vJohn.addEdge("worksOn", vProject1);
		g.tx().commit();

		System.out.println("Before commit #1: " + timestampBeforeTx1);
		System.out.println("Before commit #2: " + timestampBeforeTx2);
		System.out.println("Before commit #3: " + timestampBeforeTx3);

		// open a transaction before the first commit and assert that the graph is empty
		{
			g.tx().open(timestampBeforeTx1);
			assertEquals(timestampBeforeTx1, g.tx().getCurrentTransaction().getTimestamp());
			this.assertVertexAndEdgeCountEquals(g, 0, 0);
			// assert that the histories are empty
			List<Long> historyMartin = Lists.newArrayList(g.getVertexHistory(vMartinId));
			List<Long> historyJohn = Lists.newArrayList(g.getVertexHistory(vJohnId));
			assertEquals(0, historyMartin.size());
			assertEquals(0, historyJohn.size());
			// close the transaction again
			g.tx().close();
		}
		// open a second transaction after the first commit and assert graph integrity
		{
			g.tx().open(timestampBeforeTx2);
			assertEquals(timestampBeforeTx2, g.tx().getCurrentTransaction().getTimestamp());
			this.assertVertexAndEdgeCountEquals(g, 5, 4);
			this.assertVertexExists(g, vMartinId);
			this.assertVertexExists(g, vJohnId);
			this.assertVertexExists(g, vProject1Id);
			this.assertVertexExists(g, vProject2Id);
			this.assertVertexExists(g, vProject3Id);
			Vertex martin = this.getVertex(g, vMartinId);
			Vertex john = this.getVertex(g, vJohnId);
			Vertex project1 = this.getVertex(g, vProject1Id);
			Vertex project2 = this.getVertex(g, vProject2Id);
			Vertex project3 = this.getVertex(g, vProject3Id);
			this.assertEdgeExists(martin, john, "knows");
			this.assertEdgeExists(martin, project1, "worksOn");
			this.assertEdgeExists(martin, project2, "worksOn");
			this.assertEdgeExists(john, project3, "worksOn");
			// assert that the histories are correct
			List<Long> historyMartin = Lists.newArrayList(g.getVertexHistory(vMartin));
			List<Long> historyJohn = Lists.newArrayList(g.getVertexHistory(vJohn));
			assertEquals(1, historyMartin.size());
			assertEquals(1, historyJohn.size());
			assertTrue(Iterables.getOnlyElement(historyMartin) > timestampBeforeTx1);
			assertTrue(Iterables.getOnlyElement(historyMartin) <= timestampBeforeTx2);
			assertTrue(Iterables.getOnlyElement(historyMartin) < timestampBeforeTx3);
			assertTrue(Iterables.getOnlyElement(historyJohn) > timestampBeforeTx1);
			assertTrue(Iterables.getOnlyElement(historyJohn) <= timestampBeforeTx2);
			assertTrue(Iterables.getOnlyElement(historyJohn) < timestampBeforeTx3);
			// close the transaction again
			g.tx().close();
		}
		// open a third transaction after the second commit and assert graph integrity
		{
			g.tx().open(timestampBeforeTx3);
			assertEquals(timestampBeforeTx3, g.tx().getCurrentTransaction().getTimestamp());
			this.assertVertexAndEdgeCountEquals(g, 5, 5);
			this.assertVertexExists(g, vMartinId);
			this.assertVertexExists(g, vJohnId);
			this.assertVertexExists(g, vProject1Id);
			this.assertVertexNotExists(g, vProject2Id);
			this.assertVertexExists(g, vProject3Id);
			this.assertVertexExists(g, vProject4Id);
			Vertex martin = this.getVertex(g, vMartinId);
			Vertex john = this.getVertex(g, vJohnId);
			Vertex project1 = this.getVertex(g, vProject1Id);
			Vertex project3 = this.getVertex(g, vProject3Id);
			Vertex project4 = this.getVertex(g, vProject4Id);
			this.assertEdgeExists(martin, john, "knows");
			this.assertEdgeExists(martin, project1, "worksOn");
			this.assertEdgeExists(martin, project3, "worksOn");
			this.assertEdgeExists(martin, project4, "worksOn");
			this.assertEdgeExists(john, project3, "worksOn");
			// assert that the histories are correct
			List<Long> historyMartin = Lists.newArrayList(g.getVertexHistory(vMartin));
			List<Long> historyJohn = Lists.newArrayList(g.getVertexHistory(vJohn));
			assertEquals(2, historyMartin.size());
			assertEquals(1, historyJohn.size());
			// close the transaction again
			g.tx().close();
		}
		// open one last transaction to verify the final graph state
		{
			g.tx().open();
			this.assertVertexAndEdgeCountEquals(g, 5, 5);
			this.assertVertexExists(g, vMartinId);
			this.assertVertexExists(g, vJohnId);
			this.assertVertexExists(g, vProject1Id);
			this.assertVertexNotExists(g, vProject2Id);
			this.assertVertexExists(g, vProject3Id);
			this.assertVertexExists(g, vProject4Id);
			Vertex martin = this.getVertex(g, vMartinId);
			Vertex john = this.getVertex(g, vJohnId);
			Vertex project1 = this.getVertex(g, vProject1Id);
			Vertex project3 = this.getVertex(g, vProject3Id);
			Vertex project4 = this.getVertex(g, vProject4Id);
			this.assertEdgeExists(martin, john, "knows");
			this.assertEdgeExists(martin, project3, "worksOn");
			this.assertEdgeExists(martin, project4, "worksOn");
			this.assertEdgeExists(john, project1, "worksOn");
			this.assertEdgeExists(john, project3, "worksOn");
			// assert that the histories are correct
			List<Long> historyMartin = Lists.newArrayList(g.getVertexHistory(vMartin));
			List<Long> historyJohn = Lists.newArrayList(g.getVertexHistory(vJohn));
			assertEquals(3, historyMartin.size());
			assertEquals(2, historyJohn.size());
			g.tx().close();
		}
	}

	private void assertVertexAndEdgeCountEquals(final Graph g, final int vertexCount, final int edgeCount) {
		Set<Vertex> vertices = Sets.newHashSet(g.vertices());
		Set<Edge> edges = Sets.newHashSet(g.edges());
		assertEquals(vertexCount, vertices.size());
		assertEquals(edgeCount, edges.size());
	}

	private void assertEdgeExists(final Vertex outV, final Vertex inV, final String label) {
		Iterator<Edge> iterator = outV.edges(Direction.OUT, label);
		Edge edge = Iterators.getOnlyElement(Iterators.filter(iterator, e -> e.inVertex().equals(inV)), null);
		assertNotNull(edge);
		assertFalse(((ChronoEdge) edge).isRemoved());
		iterator = inV.edges(Direction.IN, label);
		edge = Iterators.getOnlyElement(Iterators.filter(iterator, e -> e.outVertex().equals(outV)), null);
		assertNotNull(edge);
		assertFalse(((ChronoEdge) edge).isRemoved());
	}

	private void assertVertexExists(final Graph g, final String vertexId) {
		Vertex vertex = this.getVertex(g, vertexId);
		assertNotNull(vertex);
		assertFalse(((ChronoVertex) vertex).isRemoved());
	}

	private void assertVertexNotExists(final Graph g, final String vertexId) {
		Vertex vertex = this.getVertex(g, vertexId);
		if (vertex == null) {
			return;
		}
		assertTrue(((ChronoVertex) vertex).isRemoved());
	}

	private Vertex getVertex(final Graph g, final String id) {
		return Iterators.getOnlyElement(g.vertices(id), null);
	}

}
