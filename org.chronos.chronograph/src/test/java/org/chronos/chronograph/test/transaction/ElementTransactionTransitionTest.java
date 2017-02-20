package org.chronos.chronograph.test.transaction;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.internal.impl.transaction.GraphTransactionContext;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class ElementTransactionTransitionTest extends AllChronoGraphBackendsTest {

	@Test
	public void vertexCanBeAccessedAfterCommit() {
		ChronoGraph g = this.getGraph();
		Vertex v = g.addVertex("name", "martin");
		g.tx().commit();
		// now we have a new transaction, but we can still use the vertex
		assertEquals("martin", v.value("name"));
	}

	@Test
	public void transientVertexModificationsAreResetOnRollback() {
		ChronoGraph g = this.getGraph();
		Vertex v = g.addVertex("name", "martin");
		g.tx().commit();
		// now modify the vertex
		v.property("name", "john");
		// assert that the transient modification is applied
		assertEquals("john", v.value("name"));
		// perform the rollback
		g.tx().rollback();
		// assert that the vertex is back in its old state
		assertEquals("martin", v.value("name"));
	}

	@Test
	public void removedVertexCanBeAccessedAfterCommitButIsInRemovedState() {
		ChronoGraph g = this.getGraph();
		Vertex v = g.addVertex("name", "martin");
		g.tx().commit();
		// remove the vertex
		v.remove();
		g.tx().commit();
		// assert that the vertex is still present, but removed
		try {
			v.value("name");
			fail("Removed vertex is still accessible!");
		} catch (NoSuchElementException expected) {
			// ok
		}
	}

	@Test
	public void canReuseVerticesAfterTransactionCommit() {
		ChronoGraph g = this.getGraph();
		Vertex vMartin1 = g.addVertex("name", "martin", "age", 26);
		Vertex vJohn1 = g.addVertex("name", "john", "age", 20);
		g.tx().commit();

		// now, retrieve the vertices via queries
		Vertex vMartin2 = Iterables.getOnlyElement(g.traversal().V().has("name", "martin").toSet());
		Vertex vJohn2 = Iterables.getOnlyElement(g.traversal().V().has("name", "john").toSet());
		assertNotNull(vMartin2);
		assertNotNull(vJohn2);

		// they must be equal to their predecessors
		assertEquals(vMartin1, vMartin2);
		assertEquals(vJohn1, vJohn2);

		// changing the successor must update the predecessor
		vMartin2.property("age", 30);
		assertEquals(30, (int) vMartin1.value("age"));

		// changing the predecessor must update the successor
		vJohn1.property("age", 21);
		assertEquals(21, (int) vJohn2.value("age"));
	}

	@Test
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_AUTO_OPEN, value = "false")
	public void canReuseVerticesAfterIncrementalCommit() {
		ChronoGraph graph = this.getGraph();
		// open a new graph transaction
		graph.tx().open();

		// extend the graph
		Vertex v2 = graph.addVertex(T.id, "v2", "firstname", "Jane", "lastname", "Smith");
		Vertex v3 = graph.addVertex(T.id, "v3", "firstname", "Jill", "lastname", "Johnson");

		// {
		// System.out.println("BEFORE ROLLOVER");
		// ChronoVertexProxy v2Proxy = (ChronoVertexProxy) v2;
		// ChronoVertexImpl v2Impl = v2Proxy.getElement();
		// ChronoVertexProxy v2ProxyReloaded = (ChronoVertexProxy) graph.vertices(v2Proxy).next();
		// ChronoVertexImpl v2ImplReloaded = v2ProxyReloaded.getElement();
		// System.out.println(
		// "Vertex [v2]: ProxyID=" + hexId(v2Proxy) + ", VertexID=" + hexId(v2Impl) + ", Refreshed ProxyID="
		// + hexId(v2ProxyReloaded) + ", Refreshed VertexID=" + hexId(v2ImplReloaded));
		// ChronoVertexProxy v3Proxy = (ChronoVertexProxy) v3;
		// ChronoVertexImpl v3Impl = v3Proxy.getElement();
		// ChronoVertexProxy v3ProxyReloaded = (ChronoVertexProxy) graph.vertices(v3Proxy).next();
		// ChronoVertexImpl v3ImplReloaded = v3ProxyReloaded.getElement();
		// System.out.println(
		// "Vertex [v3]: ProxyID=" + hexId(v3Proxy) + ", VertexID=" + hexId(v3Impl) + ", Refreshed ProxyID="
		// + hexId(v3ProxyReloaded) + ", Refreshed VertexID=" + hexId(v3ImplReloaded));
		// }

		assertEquals(1, graph.find().vertices().where("firstname").isEqualTo("Jane").count());

		// perform the incremental commit
		graph.tx().commitIncremental();

		// jane should exist
		assertEquals(v2, graph.traversal().V(v2).next());
		// jill should exist
		assertEquals(v3, graph.traversal().V(v3).next());

		assertEquals(1, graph.find().vertices().where("firstname").isEqualTo("Jane").count());

		// {
		// System.out.println("AFTER ROLLOVER, BEFORE ADD EDGE");
		// ChronoVertexProxy v2Proxy = (ChronoVertexProxy) v2;
		// ChronoVertexImpl v2Impl = v2Proxy.getElement();
		// ChronoVertexProxy v2ProxyReloaded = (ChronoVertexProxy) graph.vertices(v2Proxy).next();
		// ChronoVertexImpl v2ImplReloaded = v2ProxyReloaded.getElement();
		// System.out.println(
		// "Vertex [v2]: ProxyID=" + hexId(v2Proxy) + ", VertexID=" + hexId(v2Impl) + ", Refreshed ProxyID="
		// + hexId(v2ProxyReloaded) + ", Refreshed VertexID=" + hexId(v2ImplReloaded));
		// ChronoVertexProxy v3Proxy = (ChronoVertexProxy) v3;
		// ChronoVertexImpl v3Impl = v3Proxy.getElement();
		// ChronoVertexProxy v3ProxyReloaded = (ChronoVertexProxy) graph.vertices(v3Proxy).next();
		// ChronoVertexImpl v3ImplReloaded = v3ProxyReloaded.getElement();
		// System.out.println(
		// "Vertex [v3]: ProxyID=" + hexId(v3Proxy) + ", VertexID=" + hexId(v3Impl) + ", Refreshed ProxyID="
		// + hexId(v3ProxyReloaded) + ", Refreshed VertexID=" + hexId(v3ImplReloaded));
		// }

		// create some associations
		v2.addEdge("knows", v3, "kind", "friend");
		v3.addEdge("knows", v2, "kind", "friend");

		// {
		// System.out.println("AFTER ROLLOVER, AFTER ADD EDGE");
		// ChronoVertexProxy v2Proxy = (ChronoVertexProxy) v2;
		// ChronoVertexImpl v2Impl = v2Proxy.getElement();
		// ChronoVertexProxy v2ProxyReloaded = (ChronoVertexProxy) graph.vertices(v2Proxy).next();
		// ChronoVertexImpl v2ImplReloaded = v2ProxyReloaded.getElement();
		// System.out.println(
		// "Vertex [v2]: ProxyID=" + hexId(v2Proxy) + ", VertexID=" + hexId(v2Impl) + ", Refreshed ProxyID="
		// + hexId(v2ProxyReloaded) + ", Refreshed VertexID=" + hexId(v2ImplReloaded));
		// ChronoVertexProxy v3Proxy = (ChronoVertexProxy) v3;
		// ChronoVertexImpl v3Impl = v3Proxy.getElement();
		// ChronoVertexProxy v3ProxyReloaded = (ChronoVertexProxy) graph.vertices(v3Proxy).next();
		// ChronoVertexImpl v3ImplReloaded = v3ProxyReloaded.getElement();
		// System.out.println(
		// "Vertex [v3]: ProxyID=" + hexId(v3Proxy) + ", VertexID=" + hexId(v3Impl) + ", Refreshed ProxyID="
		// + hexId(v3ProxyReloaded) + ", Refreshed VertexID=" + hexId(v3ImplReloaded));
		// }

		// jane and jill should be marked as "dirty" in the transaction context
		ChronoGraphTransaction tx = graph.tx().getCurrentTransaction();
		GraphTransactionContext context = tx.getContext();
		assertEquals(2, context.getModifiedVertices().size());

		// jane and jill should have one outgoing edge each
		assertEquals(1, Iterators.size(v2.edges(Direction.OUT, "knows")));
		assertEquals(1, Iterators.size(v3.edges(Direction.OUT, "knows")));

		// even after reloading their elements, jane and jill should STILL have one outgoing edge each
		assertEquals(1, Iterators.size(graph.vertices(v2).next().edges(Direction.OUT, "knows")));
		assertEquals(1, Iterators.size(graph.vertices(v3).next().edges(Direction.OUT, "knows")));

		// jane should know jill
		assertEquals(1, graph.traversal().V(v2).outE("knows").toSet().size());
		assertEquals(1, graph.traversal().V(v3).inE("knows").toSet().size());
		// jill should know jane
		assertEquals(1, graph.traversal().V(v3).outE("knows").toSet().size());
		assertEquals(1, graph.traversal().V(v2).inE("knows").toSet().size());

		graph.tx().commit();

		graph.tx().open();

		// jane should know jill
		assertEquals(1, graph.traversal().V(v2).outE("knows").toSet().size());
		assertEquals(1, graph.traversal().V(v3).inE("knows").toSet().size());
		// jill should know jane
		assertEquals(1, graph.traversal().V(v3).outE("knows").toSet().size());
		assertEquals(1, graph.traversal().V(v2).inE("knows").toSet().size());

		graph.tx().close();
	}

	@Test
	public void cannotAccessGraphElementFromOtherThread() {
		ChronoGraph g = this.getGraph();
		Vertex vMartin = g.addVertex("name", "martin", "age", 26);
		g.tx().commit();
		// use a thread (controlled synchronously) to access some vertex data
		Thread worker = new Thread(() -> {
			try {
				vMartin.value("name");
				fail("Managed to access vertex data from other thread!");
			} catch (IllegalStateException expected) {
				// pass
			}
		});
		// set up a root exception handler in the thread, such that our test
		// can fail properly if an error during the access in the other thread occurs
		Set<Throwable> workerThreadExceptions = Sets.newConcurrentHashSet();
		worker.setUncaughtExceptionHandler((thread, ex) -> {
			workerThreadExceptions.add(ex);
		});
		// run the worker
		worker.start();
		// wait for the worker to finish
		try {
			worker.join();
		} catch (InterruptedException e) {
			fail("Worker was interrupted! Exception: " + e);
		}
		// assert that there were no errors during the access
		assertEquals(Collections.emptySet(), workerThreadExceptions);
	}

	// private static String hexId(final Object obj) {
	// return Integer.toHexString(System.identityHashCode(obj));
	// }
}
