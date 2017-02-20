package org.chronos.chronograph.test.hashcode;

import static org.junit.Assert.*;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.junit.Test;

public class GraphElementHashCodeAndEqualsTest extends AllChronoGraphBackendsTest {

	@Test
	public void vertexHashCodeAndEqualsAreConsistent() {
		ChronoGraph graph = this.getGraph();
		Vertex v = graph.addVertex();
		assertEquals(v.hashCode(), v.id().hashCode());
		assertEquals(v, v);
		assertEquals(v.id(), v.id());
		assertNotEquals(v, v.id());
	}

	@Test
	public void edgeHashCodeAndEqualsAreConsistent() {
		ChronoGraph graph = this.getGraph();
		Vertex v = graph.addVertex();
		Edge e = v.addEdge("test", v);
		assertEquals(e.hashCode(), e.id().hashCode());
		assertEquals(e, e);
		assertEquals(e.id(), e.id());
		assertNotEquals(e, e.id());
	}

}
