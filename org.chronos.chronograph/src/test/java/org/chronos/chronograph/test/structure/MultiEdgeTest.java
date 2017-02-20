package org.chronos.chronograph.test.structure;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class MultiEdgeTest extends AllChronoGraphBackendsTest {

	@Test
	public void canHaveMultipleEdgesBetweenTwoVertices() {
		ChronoGraph g = this.getGraph();
		Vertex v1 = g.addVertex("name", "v1");
		Vertex v2 = g.addVertex("name", "v2");
		Edge e1 = v1.addEdge("ref", v2);
		Edge e2 = v1.addEdge("ref", v2);
		this.assertCommitAssert(() -> {
			// basic checks
			assertNotNull(e1);
			assertNotNull(e2);
			assertNotEquals(e1, e2);
			// check the 'edges' property
			Set<Edge> edges = Sets.newHashSet(v1.edges(Direction.OUT, "ref"));
			assertEquals(Sets.newHashSet(e1, e2), edges);
			// check the 'vertices' property with a list and assert that the duplicate is present
			List<Vertex> targetVertices = Lists.newArrayList(v1.vertices(Direction.OUT, "ref"));
			assertEquals(2, targetVertices.size());
			assertTrue(targetVertices.contains(v2));
			assertTrue(targetVertices.indexOf(v2) != targetVertices.lastIndexOf(v2));
			// check the 'vertices' property with a set and assert that the duplicate is gone
			Set<Vertex> targetVertexSet = Sets.newHashSet(v1.vertices(Direction.OUT, "ref"));
			assertEquals(Sets.newHashSet(v2), targetVertexSet);
			// check with gremlin
			Iterator<Edge> gremlin1 = g.traversal().V(v1).outE("ref");
			assertEquals(2, Iterators.size(gremlin1));
			Iterator<Vertex> gremlin2 = g.traversal().V(v1).out("ref").dedup();
			assertEquals(1, Iterators.size(gremlin2));
		});
	}

	private void assertCommitAssert(final Runnable assertion) {
		assertion.run();
		this.getGraph().tx().commit();
		assertion.run();
	}

}
