package org.chronos.chronograph.test.id;

import static org.junit.Assert.*;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class FetchElementsByIdTest extends AllChronoGraphBackendsTest {

	@Test
	public void fetchingUnknownVertexIdReturnsEmptyIterator() {
		ChronoGraph graph = this.getGraph();

		Iterator<Vertex> vertices = graph.vertices("fake");
		assertFalse(vertices.hasNext());
	}

	@Test
	public void fetchingUnknownEdgeIdReturnsEmptyIterator() {
		ChronoGraph graph = this.getGraph();

		Iterator<Edge> edges = graph.edges("fake");
		assertFalse(edges.hasNext());
	}

}
