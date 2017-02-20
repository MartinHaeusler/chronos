package org.chronos.chronograph.test.structure;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.api.transaction.ChronoGraphTransactionManager;
import org.chronos.chronograph.internal.impl.transaction.GraphTransactionContext;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class RemoveElementsTest extends AllChronoGraphBackendsTest {

	@Test
	public void removingVerticesWorks() {
		Graph g = this.getGraph();
		g.addVertex("kind", "person", "name", "martin");
		g.addVertex("kind", "person", "name", "john");
		g.tx().commit();

		// find me
		Vertex me = g.traversal().V().has("name", "martin").toSet().iterator().next();
		assertNotNull(me);

		// remove me and commit
		me.remove();
		g.tx().commit();

		// check that I'm gone
		Set<Vertex> persons = g.traversal().V().has("kind", "person").toSet();
		assertEquals(1, persons.size());
		Vertex person = Iterables.getOnlyElement(persons);
		assertEquals("john", person.value("name"));
	}

	@Test
	public void removingEdgesWorks() {
		Graph g = this.getGraph();
		Vertex me = g.addVertex("kind", "person", "name", "martin");
		Vertex john = g.addVertex("kind", "person", "name", "john");
		me.addEdge("friend", john, "since", "forever");
		g.tx().commit();

		// find the edge
		Edge friendship = g.traversal().E().has("since", "forever").toSet().iterator().next();
		assertNotNull(friendship);

		// remove the edge and commit
		friendship.remove();
		g.tx().commit();

		// check that the edge no longer exists in gremlin
		Set<Edge> edges = g.traversal().E().has("since", "forever").toSet();
		assertTrue(edges.isEmpty());

		// check that the edge no longer exists in the adjacent vertices
		Vertex me2 = g.traversal().V().has("name", "martin").toSet().iterator().next();
		Vertex john2 = g.traversal().V().has("name", "john").toSet().iterator().next();
		assertFalse(me2.edges(Direction.BOTH, "friend").hasNext());
		assertFalse(john2.edges(Direction.BOTH, "friend").hasNext());
	}

	@Test
	public void removingVertexPropertiesWorks() {
		Graph g = this.getGraph();
		g.addVertex("kind", "person", "name", "martin");
		g.tx().commit();

		Vertex me = g.traversal().V().has("name", "martin").toSet().iterator().next();
		assertNotNull(me);

		me.property("kind").remove();
		g.tx().commit();

		Vertex me2 = g.traversal().V().has("name", "martin").toSet().iterator().next();
		assertNotNull(me2);
		assertFalse(me2.property("kind").isPresent());
	}

	@Test
	public void removingVertexMetaPropertiesWorks() {
		Graph g = this.getGraph();
		Vertex me = g.addVertex("kind", "person", "name", "martin");
		me.property("kind").property("job", "researcher");
		g.tx().commit();

		Vertex me2 = g.traversal().V().has("name", "martin").toSet().iterator().next();
		assertNotNull(me2);
		assertTrue(me2.property("kind").isPresent());
		assertTrue(me2.property("kind").property("job").isPresent());
		assertEquals("person", me2.value("kind"));
		assertEquals("researcher", me2.property("kind").value("job"));

		me2.property("kind").property("job").remove();
		g.tx().commit();

		Vertex me3 = g.traversal().V().has("name", "martin").toSet().iterator().next();
		assertNotNull(me3);
		assertTrue(me3.property("kind").isPresent());
		assertFalse(me3.property("kind").property("job").isPresent());
	}

	@Test
	public void removingEdgePropertiesWorks() {
		Graph g = this.getGraph();
		Vertex me = g.addVertex("kind", "person", "name", "martin");
		Vertex john = g.addVertex("kind", "person", "name", "john");
		me.addEdge("friend", john, "since", "forever");
		g.tx().commit();

		// find the edge
		Edge friendship = g.traversal().E().has("since", "forever").toSet().iterator().next();
		assertNotNull(friendship);

		// remove the edge property and commit
		friendship.property("since").remove();
		g.tx().commit();

		// assert that the property is gone
		Vertex me2 = g.traversal().V().has("name", "martin").toSet().iterator().next();
		Iterator<Edge> edges = me2.edges(Direction.BOTH, "friend");
		assertTrue(edges.hasNext());
		Edge friendship2 = Iterators.getOnlyElement(edges);
		assertNotNull(friendship2);
		assertFalse(friendship2.property("since").isPresent());
	}

	@Test
	public void removingAVertexAlsoRemovesAdjacentEdges() {
		Graph g = this.getGraph();
		Vertex me = g.addVertex("kind", "person", "name", "martin");
		Vertex john = g.addVertex("kind", "person", "name", "john");
		me.addEdge("friend", john, "since", "forever");
		g.tx().commit();

		assertEquals(1, Iterators.size(g.edges()));

		me.remove();
		g.tx().commit();

		assertEquals(0, Iterators.size(g.edges()));
	}

	@Test
	public void removingAllVerticesAlsoRemovesAllEdges() {
		// see also: org.apache.tinkerpop.gremlin.structure.VertexTest#shouldNotGetConcurrentModificationException
		Graph g = this.getGraph();
		Vertex v1 = g.addVertex("name", "one");
		Vertex v2 = g.addVertex("name", "two");
		Vertex v3 = g.addVertex("name", "three");
		ChronoLogger.logInfo("v1 ID: " + v1.id());
		ChronoLogger.logInfo("v2 ID: " + v2.id());
		ChronoLogger.logInfo("v3 ID: " + v3.id());
		ChronoLogger.logInfo("");
		ChronoLogger.logInfo("v1->v1: " + v1.addEdge("self", v1).id());
		ChronoLogger.logInfo("v1->v2: " + v1.addEdge("knows", v2).id());
		ChronoLogger.logInfo("v1->v3: " + v1.addEdge("knows", v3).id());
		ChronoLogger.logInfo("");
		ChronoLogger.logInfo("v2->v2: " + v2.addEdge("self", v2).id());
		ChronoLogger.logInfo("v2->v1: " + v2.addEdge("knows", v1).id());
		ChronoLogger.logInfo("v2->v3: " + v2.addEdge("knows", v3).id());
		ChronoLogger.logInfo("");
		ChronoLogger.logInfo("v3->v3: " + v3.addEdge("self", v3).id());
		ChronoLogger.logInfo("v3->v1: " + v3.addEdge("knows", v1).id());
		ChronoLogger.logInfo("v3->v2: " + v3.addEdge("knows", v2).id());

		assertTrue(Sets.newHashSet(v1.vertices(Direction.OUT)).contains(v1));
		assertTrue(Sets.newHashSet(v1.vertices(Direction.OUT)).contains(v2));
		assertTrue(Sets.newHashSet(v1.vertices(Direction.OUT)).contains(v3));

		assertTrue(Sets.newHashSet(v1.vertices(Direction.IN)).contains(v1));
		assertTrue(Sets.newHashSet(v1.vertices(Direction.IN)).contains(v2));
		assertTrue(Sets.newHashSet(v1.vertices(Direction.IN)).contains(v3));

		assertTrue(Sets.newHashSet(v2.vertices(Direction.OUT)).contains(v1));
		assertTrue(Sets.newHashSet(v2.vertices(Direction.OUT)).contains(v2));
		assertTrue(Sets.newHashSet(v2.vertices(Direction.OUT)).contains(v3));

		assertTrue(Sets.newHashSet(v2.vertices(Direction.IN)).contains(v1));
		assertTrue(Sets.newHashSet(v2.vertices(Direction.IN)).contains(v2));
		assertTrue(Sets.newHashSet(v2.vertices(Direction.IN)).contains(v3));

		assertTrue(Sets.newHashSet(v3.vertices(Direction.OUT)).contains(v1));
		assertTrue(Sets.newHashSet(v3.vertices(Direction.OUT)).contains(v2));
		assertTrue(Sets.newHashSet(v3.vertices(Direction.OUT)).contains(v3));

		assertTrue(Sets.newHashSet(v3.vertices(Direction.IN)).contains(v1));
		assertTrue(Sets.newHashSet(v3.vertices(Direction.IN)).contains(v2));
		assertTrue(Sets.newHashSet(v3.vertices(Direction.IN)).contains(v3));

		g.tx().commit();

		ChronoGraphTransactionManager txManager = (ChronoGraphTransactionManager) g.tx();
		txManager.open();
		ChronoGraphTransaction tx = txManager.getCurrentTransaction();
		GraphTransactionContext context = tx.getContext();
		assertFalse(context.isDirty());

		assertTrue(Sets.newHashSet(v1.vertices(Direction.OUT)).contains(v1));
		assertTrue(Sets.newHashSet(v1.vertices(Direction.OUT)).contains(v2));
		assertTrue(Sets.newHashSet(v1.vertices(Direction.OUT)).contains(v3));

		assertTrue(Sets.newHashSet(v1.vertices(Direction.IN)).contains(v1));
		assertTrue(Sets.newHashSet(v1.vertices(Direction.IN)).contains(v2));
		assertTrue(Sets.newHashSet(v1.vertices(Direction.IN)).contains(v3));

		assertTrue(Sets.newHashSet(v2.vertices(Direction.OUT)).contains(v1));
		assertTrue(Sets.newHashSet(v2.vertices(Direction.OUT)).contains(v2));
		assertTrue(Sets.newHashSet(v2.vertices(Direction.OUT)).contains(v3));

		assertTrue(Sets.newHashSet(v2.vertices(Direction.IN)).contains(v1));
		assertTrue(Sets.newHashSet(v2.vertices(Direction.IN)).contains(v2));
		assertTrue(Sets.newHashSet(v2.vertices(Direction.IN)).contains(v3));

		assertTrue(Sets.newHashSet(v3.vertices(Direction.OUT)).contains(v1));
		assertTrue(Sets.newHashSet(v3.vertices(Direction.OUT)).contains(v2));
		assertTrue(Sets.newHashSet(v3.vertices(Direction.OUT)).contains(v3));

		assertTrue(Sets.newHashSet(v3.vertices(Direction.IN)).contains(v1));
		assertTrue(Sets.newHashSet(v3.vertices(Direction.IN)).contains(v2));
		assertTrue(Sets.newHashSet(v3.vertices(Direction.IN)).contains(v3));

		v3.remove();
		assertEquals(5, context.getModifiedEdges().size());
		assertEquals(3, context.getModifiedVertices().size());
		assertEquals(2, Iterators.size(v2.edges(Direction.OUT)));
		assertEquals(2, Iterators.size(v2.edges(Direction.IN)));
		assertEquals(2, Iterators.size(v1.edges(Direction.OUT)));
		assertEquals(2, Iterators.size(v1.edges(Direction.IN)));

		g.tx().commit();

		assertEquals(3, Iterators.size(v2.edges(Direction.BOTH)));
		assertEquals(3, Iterators.size(v1.edges(Direction.BOTH)));

		v2.remove();
		g.tx().commit();

		v1.remove();
		g.tx().commit();

		assertEquals(0, Iterators.size(g.edges()));
	}

	@Test
	public void removingVertexWithSelfEdgeWorks() {
		Graph graph = this.getGraph();
		Vertex v = graph.addVertex("name", "martin");
		v.addEdge("self", v);
		graph.tx().commit();

		v.remove();

		assertEquals(0, Iterators.size(graph.vertices()));
		assertEquals(0, Iterators.size(graph.edges()));

		graph.tx().commit();

		assertEquals(0, Iterators.size(graph.vertices()));
		assertEquals(0, Iterators.size(graph.edges()));
	}

	@Test
	public void removingSelfEdgeWorks() {
		Graph graph = this.getGraph();
		Vertex v = graph.addVertex("name", "martin");
		Edge e = v.addEdge("self", v);
		graph.tx().commit();

		e.remove();

		assertEquals(1, Iterators.size(graph.vertices()));
		assertEquals(0, Iterators.size(graph.edges()));
		assertEquals(0, Iterators.size(v.edges(Direction.OUT)));
		assertEquals(0, Iterators.size(v.edges(Direction.IN)));

		graph.tx().commit();

		assertEquals(1, Iterators.size(graph.vertices()));
		assertEquals(0, Iterators.size(graph.edges()));
		assertEquals(0, Iterators.size(v.edges(Direction.OUT)));
		assertEquals(0, Iterators.size(v.edges(Direction.IN)));
	}

	@Test
	public void removingVerticesFromSingleLinkedCyclicGraphWorks() {
		Graph g = this.getGraph();
		Vertex a = g.addVertex("name", "a");
		Vertex b = g.addVertex("name", "b");
		Vertex c = g.addVertex("name", "c");
		a.addEdge("knows", b);
		b.addEdge("knows", c);
		c.addEdge("knows", a);
		g.tx().commit();

		a.remove();

		assertEquals(2, Iterators.size(g.vertices()));
		assertEquals(1, Iterators.size(g.edges()));
		assertEquals(1, Iterators.size(b.edges(Direction.OUT)));
		assertEquals(0, Iterators.size(b.edges(Direction.IN)));
		assertEquals(0, Iterators.size(c.edges(Direction.OUT)));
		assertEquals(1, Iterators.size(c.edges(Direction.IN)));

		g.tx().commit();

		assertEquals(2, Iterators.size(g.vertices()));
		assertEquals(1, Iterators.size(g.edges()));
		assertEquals(1, Iterators.size(b.edges(Direction.OUT)));
		assertEquals(0, Iterators.size(b.edges(Direction.IN)));
		assertEquals(0, Iterators.size(c.edges(Direction.OUT)));
		assertEquals(1, Iterators.size(c.edges(Direction.IN)));
	}

	@Test
	public void removingVerticesFromDoubleLinkedCyclicGraphWorks() {
		Graph g = this.getGraph();
		Vertex a = g.addVertex("name", "a");
		Vertex b = g.addVertex("name", "b");
		Vertex c = g.addVertex("name", "c");
		a.addEdge("knows", b);
		a.addEdge("knows", c);
		b.addEdge("knows", a);
		b.addEdge("knows", c);
		c.addEdge("knows", a);
		c.addEdge("knows", b);
		g.tx().commit();

		a.remove();

		assertEquals(2, Iterators.size(g.vertices()));
		assertEquals(2, Iterators.size(g.edges()));
		assertEquals(1, Iterators.size(b.edges(Direction.OUT)));
		assertEquals(1, Iterators.size(b.edges(Direction.IN)));
		assertEquals(1, Iterators.size(c.edges(Direction.OUT)));
		assertEquals(1, Iterators.size(c.edges(Direction.IN)));

		g.tx().commit();

		assertEquals(2, Iterators.size(g.vertices()));
		assertEquals(2, Iterators.size(g.edges()));
		assertEquals(1, Iterators.size(b.edges(Direction.OUT)));
		assertEquals(1, Iterators.size(b.edges(Direction.IN)));
		assertEquals(1, Iterators.size(c.edges(Direction.OUT)));
		assertEquals(1, Iterators.size(c.edges(Direction.IN)));
	}

	@Test
	public void removingVerticesFromMaximallyConnectedGraphOneByOneWorks() {
		Graph g = this.getGraph();
		g.addVertex("name", "a");
		g.addVertex("name", "b");
		g.addVertex("name", "c");
		g.addVertex("name", "d");
		g.addVertex("name", "e");
		// for all vertices...
		g.vertices().forEachRemaining(v -> {
			// ... connect with all other vertices
			g.vertices().forEachRemaining(v2 -> {
				v.addEdge("knows", v2);
			});
		});

		assertEquals(5, Iterators.size(g.vertices()));
		assertEquals(25, Iterators.size(g.edges()));
		g.vertices().forEachRemaining(v -> {
			assertEquals(5, Iterators.size(v.edges(Direction.OUT)));
			assertEquals(5, Iterators.size(v.edges(Direction.IN)));
		});

		g.tx().commit();

		Set<Vertex> vertices = Sets.newHashSet();
		g.vertices().forEachRemaining(v -> {
			vertices.add(v);
		});

		for (Vertex vertex : vertices) {
			vertex.remove();
			g.tx().commit();
		}

		assertEquals(0, Iterators.size(g.vertices()));
		assertEquals(0, Iterators.size(g.edges()));
	}

	@Test
	public void removingVerticesFromMaximallyConnectedGraphInOneGoWorks() {
		Graph g = this.getGraph();
		g.addVertex("name", "a");
		g.addVertex("name", "b");
		g.addVertex("name", "c");
		g.addVertex("name", "d");
		g.addVertex("name", "e");
		// for all vertices...
		g.vertices().forEachRemaining(v -> {
			// ... connect with all other vertices
			g.vertices().forEachRemaining(v2 -> {
				v.addEdge("knows", v2);
			});
		});

		assertEquals(5, Iterators.size(g.vertices()));
		assertEquals(25, Iterators.size(g.edges()));
		g.vertices().forEachRemaining(v -> {
			assertEquals(5, Iterators.size(v.edges(Direction.OUT)));
			assertEquals(5, Iterators.size(v.edges(Direction.IN)));
		});

		g.tx().commit();

		g.vertices().forEachRemaining(v -> {
			v.remove();
		});

		assertEquals(0, Iterators.size(g.vertices()));
		assertEquals(0, Iterators.size(g.edges()));

		g.tx().commit();

		assertEquals(0, Iterators.size(g.vertices()));
		assertEquals(0, Iterators.size(g.edges()));

	}

	@Test
	public void removedVerticesDoNotShowUpInIndexQueryResults() {
		ChronoGraph g = this.getGraph();
		g.getIndexManager().createIndex().onVertexProperty("name").build();
		g.getIndexManager().reindexAll();
		g.tx().commit();
		Vertex vMartin = g.addVertex("name", "Martin");
		Vertex vJoe = g.addVertex("name", "Joe");
		g.tx().commit();
		// modify the vertex
		vMartin.property("name", "Joe");
		// remove the vertex
		vMartin.remove();
		// assert that we don't get the removed vertex in the result set
		Set<Vertex> queryResult = g.find().vertices().where("name").isEqualToIgnoreCase("joe").toSet();
		assertEquals(1, queryResult.size());
		assertTrue(queryResult.contains(vJoe));
		assertFalse(queryResult.contains(vMartin));
		// perform a commit and repeat the checks
		g.tx().commit();
		queryResult = g.find().vertices().where("name").isEqualToIgnoreCase("joe").toSet();
		assertEquals(1, queryResult.size());
		assertTrue(queryResult.contains(vJoe));
		assertFalse(queryResult.contains(vMartin));
	}

	@Test
	public void removedVertexIsNotReturnedByGdotV() {
		final String ID = "1234";
		ChronoGraph g = this.getGraph();
		g.addVertex(T.id, ID);

		assertNotNull(Iterators.getOnlyElement(g.vertices(ID)));
		assertFalse(((ChronoVertex) Iterators.getOnlyElement(g.vertices(ID))).isRemoved());

		g.tx().commit();

		assertNotNull(Iterators.getOnlyElement(g.vertices(ID)));
		assertFalse(((ChronoVertex) Iterators.getOnlyElement(g.vertices(ID))).isRemoved());

		// remove the vertex transiently in our transaction
		Iterators.getOnlyElement(g.vertices(ID)).remove();

		// make sure it's not there
		assertFalse(g.vertices(ID).hasNext());

		// commit and check again
		g.tx().commit();

		assertFalse(g.vertices(ID).hasNext());
	}

}
