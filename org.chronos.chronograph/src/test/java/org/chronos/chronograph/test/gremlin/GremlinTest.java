package org.chronos.chronograph.test.gremlin;

import static org.junit.Assert.*;

import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

public class GremlinTest extends AllChronoGraphBackendsTest {

	@Test
	public void basicVertexRetrievalWorks() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("name", "Martin", "age", 26);
		graph.addVertex("name", "Martin", "age", 10);
		graph.addVertex("name", "John", "age", 26);
		graph.tx().commit();
		Set<Vertex> vertices = graph.traversal().V().has("name", "Martin").has("age", 26).toSet();
		assertEquals(1, vertices.size());
		Vertex vertex = Iterables.getOnlyElement(vertices);
		assertEquals("Martin", vertex.value("name"));
		assertEquals(26, (int) vertex.value("age"));
	}

	@Test
	public void basicEgdeRetrievalWorks() {
		ChronoGraph graph = this.getGraph();
		Vertex me = graph.addVertex("kind", "person", "name", "Martin");
		Vertex chronos = graph.addVertex("kind", "project", "name", "Chronos");
		Vertex otherProject = graph.addVertex("kind", "project", "name", "Other Project");
		me.addEdge("worksOn", chronos, "since", "2015-07-30");
		me.addEdge("worksOn", otherProject, "since", "2000-01-01");
		graph.tx().commit();
		Set<Edge> edges = graph.traversal().E().has("since", "2015-07-30").toSet();
		assertEquals(1, edges.size());
	}

	@Test
	public void gremlinNavigationWorks() {
		ChronoGraph graph = this.getGraph();
		Vertex me = graph.addVertex("kind", "person", "name", "Martin");
		Vertex chronos = graph.addVertex("kind", "project", "name", "Chronos");
		Vertex otherProject = graph.addVertex("kind", "project", "name", "Other Project");
		me.addEdge("worksOn", chronos);
		me.addEdge("worksOn", otherProject);
		graph.tx().commit();
		Set<Vertex> vertices = graph.traversal().V().has("kind", "person").has("name", "Martin").out("worksOn").has("name", "Chronos").toSet();
		assertEquals(1, vertices.size());
		Vertex vertex = Iterables.getOnlyElement(vertices);
		assertEquals("Chronos", vertex.value("name"));
		assertEquals("project", vertex.value("kind"));
	}

	@Test
	public void multipleHasClausesWorksOnIndexedVertexProperty() {
		ChronoGraph graph = this.getGraph();
		ChronoGraphIndexManager indexManager = graph.getIndexManager();
		indexManager.create().stringIndex().onVertexProperty("name").build();
		indexManager.create().stringIndex().onVertexProperty("kind").build();
		indexManager.reindexAll();
		Vertex vExpected = graph.addVertex("kind", "person", "name", "Martin");
		graph.addVertex("kind", "person", "name", "Thomas");
		graph.addVertex("kind", "project", "name", "Chronos");
		graph.addVertex("kind", "project", "name", "Martin");
		// multiple HAS clauses need to be AND-connected.
		Set<Vertex> result = graph.traversal().V().has("name", "Martin").has("kind", "person").toSet();
		assertEquals(1, result.size());
		assertTrue(result.contains(vExpected));
		graph.tx().commit();
		result = graph.traversal().V().has("name", "Martin").has("kind", "person").toSet();
		assertEquals(1, result.size());
		assertTrue(result.contains(vExpected));
	}

	@Test
	public void multipleHasClausesWorksOnNonIndexedVertexProperty() {
		ChronoGraph graph = this.getGraph();
		Vertex vExpected = graph.addVertex("kind", "person", "name", "Martin");
		graph.addVertex("kind", "person", "name", "Thomas");
		graph.addVertex("kind", "project", "name", "Chronos");
		graph.addVertex("kind", "project", "name", "Martin");
		// multiple HAS clauses need to be AND-connected.
		Set<Vertex> result = graph.traversal().V().has("name", "Martin").has("kind", "person").toSet();
		assertEquals(1, result.size());
		assertTrue(result.contains(vExpected));
		graph.tx().commit();
		result = graph.traversal().V().has("name", "Martin").has("kind", "person").toSet();
		assertEquals(1, result.size());
		assertTrue(result.contains(vExpected));
	}

	@Test
	public void multipleHasClausesWorksOnMixedIndexedVertexProperty() {
		ChronoGraph graph = this.getGraph();
		ChronoGraphIndexManager indexManager = graph.getIndexManager();
		// do not index 'name', but index 'kind'
		indexManager.create().stringIndex().onVertexProperty("kind").build();
		indexManager.reindexAll();
		Vertex vExpected = graph.addVertex("kind", "person", "name", "Martin");
		graph.addVertex("kind", "person", "name", "Thomas");
		graph.addVertex("kind", "project", "name", "Chronos");
		graph.addVertex("kind", "project", "name", "Martin");
		// multiple HAS clauses need to be AND-connected.
		Set<Vertex> result = graph.traversal().V().has("name", "Martin").has("kind", "person").toSet();
		assertEquals(1, result.size());
		assertTrue(result.contains(vExpected));
		graph.tx().commit();
		result = graph.traversal().V().has("name", "Martin").has("kind", "person").toSet();
		assertEquals(1, result.size());
		assertTrue(result.contains(vExpected));
	}

	@Test
	public void multipleHasClausesWorksOnIndexedEdgeProperty() {
		ChronoGraph graph = this.getGraph();
		ChronoGraphIndexManager indexManager = graph.getIndexManager();
		indexManager.create().stringIndex().onEdgeProperty("a").build();
		indexManager.create().stringIndex().onEdgeProperty("b").build();
		indexManager.reindexAll();
		Vertex v1 = graph.addVertex();
		Vertex v2 = graph.addVertex();
		Edge eExpected = v1.addEdge("test1", v2, "a", "yes", "b", "true");
		v1.addEdge("test2", v2, "a", "no", "b", "true");
		v1.addEdge("test3", v2, "a", "yes", "b", "false");
		v1.addEdge("test4", v2, "a", "no", "b", "false");

		// multiple HAS clauses need to be AND-connected.
		Set<Edge> result = graph.traversal().E().has("a", "yes").has("b", "true").toSet();
		assertEquals(1, result.size());
		assertTrue(result.contains(eExpected));
		graph.tx().commit();
		result = graph.traversal().E().has("a", "yes").has("b", "true").toSet();
		assertEquals(1, result.size());
		assertTrue(result.contains(eExpected));
	}

	@Test
	public void multipleHasClausesWorksOnMixedEdgeProperty() {
		ChronoGraph graph = this.getGraph();
		ChronoGraphIndexManager indexManager = graph.getIndexManager();
		// do not index 'a', but index 'b'
		indexManager.create().stringIndex().onEdgeProperty("b").build();
		indexManager.reindexAll();
		Vertex v1 = graph.addVertex();
		Vertex v2 = graph.addVertex();
		Edge eExpected = v1.addEdge("test1", v2, "a", "yes", "b", "true");
		v1.addEdge("test2", v2, "a", "no", "b", "true");
		v1.addEdge("test3", v2, "a", "yes", "b", "false");
		v1.addEdge("test4", v2, "a", "no", "b", "false");

		// multiple HAS clauses need to be AND-connected.
		Set<Edge> result = graph.traversal().E().has("a", "yes").has("b", "true").toSet();
		assertEquals(1, result.size());
		assertTrue(result.contains(eExpected));
		graph.tx().commit();
		result = graph.traversal().E().has("a", "yes").has("b", "true").toSet();
		assertEquals(1, result.size());
		assertTrue(result.contains(eExpected));
	}

	@Test
	public void multipleHasClausesWorksOnNonIndexedEdgeProperty() {
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex();
		Vertex v2 = graph.addVertex();
		Edge eExpected = v1.addEdge("test1", v2, "a", "yes", "b", "true");
		v1.addEdge("test2", v2, "a", "no", "b", "true");
		v1.addEdge("test3", v2, "a", "yes", "b", "false");
		v1.addEdge("test4", v2, "a", "no", "b", "false");

		// multiple HAS clauses need to be AND-connected.
		Set<Edge> result = graph.traversal().E().has("a", "yes").has("b", "true").toSet();
		assertEquals(1, result.size());
		assertTrue(result.contains(eExpected));
		graph.tx().commit();
		result = graph.traversal().E().has("a", "yes").has("b", "true").toSet();
		assertEquals(1, result.size());
		assertTrue(result.contains(eExpected));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void usingSubqueriesWorks() {
		ChronoGraph graph = this.getGraph();
		Vertex vMother = graph.addVertex("name", "Eva");
		Vertex vFather = graph.addVertex("name", "Adam");
		Vertex vSon1 = graph.addVertex("name", "Kain");
		Vertex vSon2 = graph.addVertex("name", "Abel");
		Vertex vDaughter = graph.addVertex("name", "Sarah");
		vMother.addEdge("married", vFather);
		vFather.addEdge("married", vMother);
		vMother.addEdge("son", vSon1);
		vMother.addEdge("son", vSon2);
		vMother.addEdge("daughter", vDaughter);
		vFather.addEdge("son", vSon1);
		vFather.addEdge("son", vSon2);
		vFather.addEdge("daughter", vDaughter);
		Set<Vertex> vertices = graph.traversal().V(vMother).union(__.out("son"), __.out("daughter")).toSet();
		assertEquals(Sets.newHashSet(vSon1, vSon2, vDaughter), vertices);
	}

	@Test
	public void canExecuteGraphEHasLabel() {
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex();
		Vertex v2 = graph.addVertex();
		Vertex v3 = graph.addVertex();
		v1.addEdge("forward", v2);
		v2.addEdge("forward", v3);
		v3.addEdge("forward", v1);
		v1.addEdge("backward", v3);
		v2.addEdge("backward", v2);
		v3.addEdge("backward", v2);
		assertEquals(3, Iterators.size(graph.traversal().E().hasLabel("forward")));
		assertEquals(3, Iterators.size(graph.traversal().E().hasLabel("backward")));
		graph.tx().commit();
		assertEquals(3, Iterators.size(graph.traversal().E().hasLabel("forward")));
		assertEquals(3, Iterators.size(graph.traversal().E().hasLabel("backward")));
	}

	@Test
	public void canExecuteGraphVHasLabel() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex(T.label, "Person");
		graph.addVertex(T.label, "Person");
		graph.addVertex(T.label, "Location");
		assertEquals(2, Iterators.size(graph.traversal().V().hasLabel("Person")));
		assertEquals(1, Iterators.size(graph.traversal().V().hasLabel("Location")));
		graph.tx().commit();
		assertEquals(2, Iterators.size(graph.traversal().V().hasLabel("Person")));
		assertEquals(1, Iterators.size(graph.traversal().V().hasLabel("Location")));

	}
}
