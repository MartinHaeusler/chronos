package org.chronos.chronograph.test.structure;

import static org.junit.Assert.*;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterators;

@Category(IntegrationTest.class)
public class AddEdgeTest extends AllChronoGraphBackendsTest {

	@Test
	public void addingEdgesWorks() {
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex("name", "v1");
		Vertex v2 = graph.addVertex("name", "v2");
		v1.addEdge("test", v2);
		graph.tx().commit();

		Vertex v1new = Iterators.getOnlyElement(graph.vertices(v1));
		assertNotNull(v1new);

		Vertex v2new = Iterators.getOnlyElement(graph.vertices(v2));
		assertNotNull(v2new);

		Edge edge = Iterators.getOnlyElement(v1new.edges(Direction.OUT, "test"));
		assertNotNull(edge);

		assertEquals(v1new, edge.outVertex());
		assertEquals(v2new, edge.inVertex());
	}

	@Test
	public void assigningEdgePropertyAfterCreationWorks() {
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex("name", "v1");
		Vertex v2 = graph.addVertex("name", "v2");
		Edge e = v1.addEdge("test", v2);
		e.property("kind", "testificate");
		graph.tx().commit();

		Vertex v1new = Iterators.getOnlyElement(graph.vertices(v1));
		assertNotNull(v1new);

		Vertex v2new = Iterators.getOnlyElement(graph.vertices(v2));
		assertNotNull(v2new);

		Edge edge = Iterators.getOnlyElement(v1new.edges(Direction.OUT, "test"));
		assertNotNull(edge);

		assertEquals(v1new, edge.outVertex());
		assertEquals(v2new, edge.inVertex());
		assertEquals("testificate", edge.value("kind"));
	}

	@Test
	public void creatingEdgeFromNewToExistingVertexWorks() {
		ChronoGraph graph = this.getGraph();
		Vertex v2 = graph.addVertex("name", "v2");
		graph.tx().commit();

		Vertex v1 = graph.addVertex("name", "v2");
		Edge e = v1.addEdge("test", v2);
		e.property("kind", "testificate");
		graph.tx().commit();

		Vertex v1new = Iterators.getOnlyElement(graph.vertices(v1));
		assertNotNull(v1new);

		Vertex v2new = Iterators.getOnlyElement(graph.vertices(v2));
		assertNotNull(v2new);

		Edge edge = Iterators.getOnlyElement(v1new.edges(Direction.OUT, "test"));
		assertNotNull(edge);

		assertEquals(v1new, edge.outVertex());
		assertEquals(v2new, edge.inVertex());
		assertEquals("testificate", edge.value("kind"));
	}

	@Test
	public void creatingEdgeFromExistingToNewVertexWorks() {
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex("name", "v1");
		graph.tx().commit();

		Vertex v2 = graph.addVertex("name", "v2");
		Edge e = v1.addEdge("test", v2);
		e.property("kind", "testificate");
		graph.tx().commit();

		Vertex v1new = Iterators.getOnlyElement(graph.vertices(v1));
		assertNotNull(v1new);

		Vertex v2new = Iterators.getOnlyElement(graph.vertices(v2));
		assertNotNull(v2new);

		Edge edge = Iterators.getOnlyElement(v1new.edges(Direction.OUT, "test"));
		assertNotNull(edge);

		assertEquals(v1new, edge.outVertex());
		assertEquals(v2new, edge.inVertex());
		assertEquals("testificate", edge.value("kind"));
	}

	@Test
	public void redirectingEdgeByRemovingAndAddingEdgeFromExistingToNewWorks() {
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex("name", "v1");
		Vertex v2 = graph.addVertex("name", "v2");
		Edge e = v1.addEdge("test", v2);
		e.property("kind", "testificate");
		graph.tx().commit();

		// create a new vertex, v3
		Vertex v3 = graph.addVertex("name", "v3");
		// remove the old edge between v1 and v2
		Vertex v1new = Iterators.getOnlyElement(graph.vertices(v1));
		assertNotNull(v1new);
		Edge oldEdge = Iterators.getOnlyElement(v1new.edges(Direction.OUT, "test"));
		assertNotNull(oldEdge);
		assertEquals("testificate", oldEdge.value("kind"));
		oldEdge.remove();

		// add a new edge from v1 to v3, with the same label and properties
		Edge newEdge = v1new.addEdge("test", v3);
		newEdge.property("kind", "testificate");

		graph.tx().commit();

		// load the graph contents again after committing the transaction
		v1 = graph.vertices(v1).next();
		v2 = graph.vertices(v2).next();
		v3 = graph.vertices(v3).next();

		// v1 should have 1 outgoing edge (to v3), and none incoming
		assertEquals(1, Iterators.size(v1.edges(Direction.OUT)));
		assertEquals(0, Iterators.size(v1.edges(Direction.IN)));
		assertEquals(v1, Iterators.getOnlyElement(v1.edges(Direction.OUT)).outVertex());
		assertEquals(v3, Iterators.getOnlyElement(v1.edges(Direction.OUT)).inVertex());

		// v2 should have no incoming or outgoing edges
		assertEquals(0, Iterators.size(v2.edges(Direction.OUT)));
		assertEquals(0, Iterators.size(v2.edges(Direction.IN)));

		// v3 should have one incoming edge (from v1) and no outgoing edges
		assertEquals(0, Iterators.size(v3.edges(Direction.OUT)));
		assertEquals(1, Iterators.size(v3.edges(Direction.IN)));
		assertEquals(v1, Iterators.getOnlyElement(v3.edges(Direction.IN)).outVertex());
		assertEquals(v3, Iterators.getOnlyElement(v3.edges(Direction.IN)).inVertex());

		// load the edge
		Edge edge = v1.edges(Direction.OUT, "test").next();
		assertNotNull(edge);
		assertEquals("testificate", edge.value("kind"));
		assertEquals(v1, edge.outVertex());
		assertEquals(v3, edge.inVertex());

		// roll back the transaction and re-load the edge from the "other side"
		graph.tx().rollback();

		v1 = graph.vertices(v1).next();
		v2 = graph.vertices(v2).next();
		v3 = graph.vertices(v3).next();
		edge = v3.edges(Direction.IN, "test").next();
		assertNotNull(edge);
		assertEquals("testificate", edge.value("kind"));

		// assert that the edge has the correct neighoring vertices
		assertEquals(v1, edge.outVertex());
		assertEquals(v3, edge.inVertex());
	}

	@Test
	public void redirectingEdgeByRemovingAndAddingEdgeFromExistingToExistingWorks() {
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex("name", "v1");
		Vertex v2 = graph.addVertex("name", "v2");
		Vertex v3 = graph.addVertex("name", "v3");
		Edge e = v1.addEdge("test", v2);
		e.property("kind", "testificate");
		graph.tx().commit();

		// remove the old edge between v1 and v2
		Vertex v1new = Iterators.getOnlyElement(graph.vertices(v1));
		assertNotNull(v1new);
		Edge oldEdge = Iterators.getOnlyElement(v1new.edges(Direction.OUT, "test"));
		assertNotNull(oldEdge);
		assertEquals("testificate", oldEdge.value("kind"));
		oldEdge.remove();

		// add a new edge from v1 to v3, with the same label and properties
		Edge newEdge = v1new.addEdge("test", v3);
		newEdge.property("kind", "testificate");

		graph.tx().commit();

		// load the graph contents again after committing the transaction
		v1 = graph.vertices(v1).next();
		v2 = graph.vertices(v2).next();
		v3 = graph.vertices(v3).next();

		// v1 should have 1 outgoing edge (to v3), and none incoming
		assertEquals(1, Iterators.size(v1.edges(Direction.OUT)));
		assertEquals(0, Iterators.size(v1.edges(Direction.IN)));
		assertEquals(v1, Iterators.getOnlyElement(v1.edges(Direction.OUT)).outVertex());
		assertEquals(v3, Iterators.getOnlyElement(v1.edges(Direction.OUT)).inVertex());

		// v2 should have no incoming or outgoing edges
		assertEquals(0, Iterators.size(v2.edges(Direction.OUT)));
		assertEquals(0, Iterators.size(v2.edges(Direction.IN)));

		// v3 should have one incoming edge (from v1) and no outgoing edges
		assertEquals(0, Iterators.size(v3.edges(Direction.OUT)));
		assertEquals(1, Iterators.size(v3.edges(Direction.IN)));
		assertEquals(v1, Iterators.getOnlyElement(v3.edges(Direction.IN)).outVertex());
		assertEquals(v3, Iterators.getOnlyElement(v3.edges(Direction.IN)).inVertex());

		// load the edge
		Edge edge = v1.edges(Direction.OUT, "test").next();
		assertNotNull(edge);
		assertEquals("testificate", edge.value("kind"));
		assertEquals(v1, edge.outVertex());
		assertEquals(v3, edge.inVertex());

		// roll back the transaction and re-load the edge from the "other side"
		graph.tx().rollback();

		v1 = graph.vertices(v1).next();
		v2 = graph.vertices(v2).next();
		v3 = graph.vertices(v3).next();
		edge = v3.edges(Direction.IN, "test").next();
		assertNotNull(edge);
		assertEquals("testificate", edge.value("kind"));

		// assert that the edge has the correct neighoring vertices
		assertEquals(v1, edge.outVertex());
		assertEquals(v3, edge.inVertex());
	}

	@Test
	public void redirectingEdgeByRemovingAndAddingEdgeFromNewToExistingWorks() {
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex("name", "v1");
		Vertex v2 = graph.addVertex("name", "v2");
		Edge e = v1.addEdge("test", v2);
		e.property("kind", "testificate");
		graph.tx().commit();

		// create a new vertex, v3
		Vertex v3 = graph.addVertex("name", "v3");
		// remove the old edge between v1 and v2
		Vertex v1new = Iterators.getOnlyElement(graph.vertices(v1));
		assertNotNull(v1new);
		Edge oldEdge = Iterators.getOnlyElement(v1new.edges(Direction.OUT, "test"));
		assertNotNull(oldEdge);
		assertEquals("testificate", oldEdge.value("kind"));
		oldEdge.remove();

		// add a new edge from v3 to v1, with the same label and properties
		Edge newEdge = v3.addEdge("test", v1);
		newEdge.property("kind", "testificate");

		graph.tx().commit();

		// load the graph contents again after committing the transaction
		v1 = graph.vertices(v1).next();
		v2 = graph.vertices(v2).next();
		v3 = graph.vertices(v3).next();

		// v1 should have 0 outgoing edge, and 1 incoming (from v3)
		assertEquals(0, Iterators.size(v1.edges(Direction.OUT)));
		assertEquals(1, Iterators.size(v1.edges(Direction.IN)));
		assertEquals(v3, Iterators.getOnlyElement(v1.edges(Direction.IN)).outVertex());
		assertEquals(v1, Iterators.getOnlyElement(v1.edges(Direction.IN)).inVertex());

		// v2 should have no incoming or outgoing edges
		assertEquals(0, Iterators.size(v2.edges(Direction.OUT)));
		assertEquals(0, Iterators.size(v2.edges(Direction.IN)));

		// v3 should have one outgoing edge (to v1) and no incoming edges
		assertEquals(1, Iterators.size(v3.edges(Direction.OUT)));
		assertEquals(0, Iterators.size(v3.edges(Direction.IN)));
		assertEquals(v3, Iterators.getOnlyElement(v3.edges(Direction.OUT)).outVertex());
		assertEquals(v1, Iterators.getOnlyElement(v3.edges(Direction.OUT)).inVertex());

		// load the edge
		Edge edge = v3.edges(Direction.OUT, "test").next();
		assertNotNull(edge);
		assertEquals("testificate", edge.value("kind"));
	}

	@Test
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
	public void removingAndAddingEdgeWithSameIdWorks() {
		String edgeID = "MyId";
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex("name", "v1");
		Vertex v2 = graph.addVertex("name", "v2");
		Vertex v3 = graph.addVertex("name", "v3");
		Edge e = v1.addEdge("test", v2, T.id, edgeID);
		e.property("kind", "testificate");
		graph.tx().commit();

		// remove the old edge between v1 and v2
		Vertex v1new = Iterators.getOnlyElement(graph.vertices(v1));
		assertNotNull(v1new);
		Edge oldEdge = Iterators.getOnlyElement(v1new.edges(Direction.OUT, "test"));
		assertNotNull(oldEdge);
		assertEquals("testificate", oldEdge.value("kind"));
		oldEdge.remove();

		// add a new edge from v1 to v3, with the same label and properties
		Edge newEdge = v1new.addEdge("test", v3, T.id, edgeID);
		newEdge.property("kind", "testificate");

		graph.tx().commit();

		// load the graph contents again after committing the transaction
		v1 = graph.vertices(v1).next();
		v2 = graph.vertices(v2).next();
		v3 = graph.vertices(v3).next();

		// v1 should have 1 outgoing edge (to v3), and none incoming
		assertEquals(1, Iterators.size(v1.edges(Direction.OUT)));
		assertEquals(0, Iterators.size(v1.edges(Direction.IN)));
		assertEquals(v1, Iterators.getOnlyElement(v1.edges(Direction.OUT)).outVertex());
		assertEquals(v3, Iterators.getOnlyElement(v1.edges(Direction.OUT)).inVertex());

		// v2 should have no incoming or outgoing edges
		assertEquals(0, Iterators.size(v2.edges(Direction.OUT)));
		assertEquals(0, Iterators.size(v2.edges(Direction.IN)));

		// v3 should have one incoming edge (from v1) and no outgoing edges
		assertEquals(0, Iterators.size(v3.edges(Direction.OUT)));
		assertEquals(1, Iterators.size(v3.edges(Direction.IN)));
		assertEquals(v1, Iterators.getOnlyElement(v3.edges(Direction.IN)).outVertex());
		assertEquals(v3, Iterators.getOnlyElement(v3.edges(Direction.IN)).inVertex());

		// load the edge
		Edge edge = v1.edges(Direction.OUT, "test").next();
		assertNotNull(edge);
		assertEquals("testificate", edge.value("kind"));

		// roll back the transaction and re-load the edge from the "other side"
		graph.tx().rollback();

		v3 = graph.vertices(v3).next();
		edge = v3.edges(Direction.IN, "test").next();
		assertNotNull(edge);
		assertEquals("testificate", edge.value("kind"));
	}

	@Test
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
	public void removingAndReaddingSameEdgeWithSameIdWorks() {
		String edgeID = "MyId";
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex("name", "v1");
		Vertex v2 = graph.addVertex("name", "v2");
		Edge e = v1.addEdge("test", v2, T.id, edgeID);
		e.property("kind", "testificate");
		graph.tx().commit();

		// remove the old edge between v1 and v2
		Vertex v1new = Iterators.getOnlyElement(graph.vertices(v1));
		assertNotNull(v1new);
		Edge oldEdge = Iterators.getOnlyElement(v1new.edges(Direction.OUT, "test"));
		assertNotNull(oldEdge);
		assertEquals("testificate", oldEdge.value("kind"));
		oldEdge.remove();

		// add a new edge from v1 to v3, with the same label and properties
		Edge newEdge = v1new.addEdge("test", v2, T.id, edgeID);
		newEdge.property("kind", "testificate");

		graph.tx().commit();

		// load the graph contents again after committing the transaction
		v1 = graph.vertices(v1).next();
		v2 = graph.vertices(v2).next();

		// v1 should have 1 outgoing edge (to v3), and none incoming
		assertEquals(1, Iterators.size(v1.edges(Direction.OUT)));
		assertEquals(0, Iterators.size(v1.edges(Direction.IN)));

		// v2 should have 1 incoming and 0 outgoing edges
		assertEquals(0, Iterators.size(v2.edges(Direction.OUT)));
		assertEquals(1, Iterators.size(v2.edges(Direction.IN)));

		// load the edge
		Edge edge = v1.edges(Direction.OUT, "test").next();
		assertNotNull(edge);
		assertEquals("testificate", edge.value("kind"));

		// roll back the transaction and re-load the edge from the "other side"
		graph.tx().rollback();

		v2 = graph.vertices(v2).next();
		edge = v2.edges(Direction.IN, "test").next();
		assertNotNull(edge);
		assertEquals("testificate", edge.value("kind"));
	}

}
