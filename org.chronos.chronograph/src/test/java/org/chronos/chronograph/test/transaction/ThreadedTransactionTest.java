package org.chronos.chronograph.test.transaction;

import static org.junit.Assert.*;

import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class ThreadedTransactionTest extends AllChronoGraphBackendsTest {

	@Test
	public void canOpenThreadedTransaction() {
		ChronoGraph graph = this.getGraph();
		ChronoGraph threadedTx = graph.tx().createThreadedTx();
		assertNotNull(threadedTx);
	}

	@Test
	public void canAccessGraphElementsInThreadedTransaction() {
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex("name", "Martin");
		Vertex v2 = graph.addVertex("name", "John");
		Edge e = v1.addEdge("knows", v2);
		graph.tx().commit();
		// open the threaded transaction
		ChronoGraph threadedTx = graph.tx().createThreadedTx();
		assertNotNull(threadedTx);
		// try to find the elements
		Vertex threadedV1 = Iterators.getOnlyElement(threadedTx.vertices(v1));
		Vertex threadedV2 = Iterators.getOnlyElement(threadedTx.vertices(v2));
		Edge threadedE = Iterators.getOnlyElement(threadedTx.edges(e));
		// assert that the elements are equal...
		assertEquals(v1, threadedV1);
		assertEquals(v2, threadedV2);
		assertEquals(e, threadedE);
		// ... but not the same w.r.t. ==
		assertFalse(v1 == threadedV1);
		assertFalse(v2 == threadedV2);
		assertFalse(e == threadedE);
	}

	@Test
	public void graphElementsInThreadedTransactionCanBeAccessedInAnyThread() {
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex("name", "Martin");
		Vertex v2 = graph.addVertex("name", "John");
		Edge e = v1.addEdge("knows", v2);
		graph.tx().commit();
		// open the threaded transaction
		ChronoGraph threadedTx = graph.tx().createThreadedTx();
		assertNotNull(threadedTx);
		// try to find the elements
		Vertex threadedV1 = Iterators.getOnlyElement(threadedTx.vertices(v1));
		Vertex threadedV2 = Iterators.getOnlyElement(threadedTx.vertices(v2));
		Edge threadedE = Iterators.getOnlyElement(threadedTx.edges(e));
		// use a thread (controlled synchronously) to access some vertex data
		this.executeSynchronouslyInWorkerThread(() -> {
			assertEquals("Martin", threadedV1.value("name"));
			assertEquals("John", threadedV2.value("name"));
		});
		// ... and do the same for edges
		this.executeSynchronouslyInWorkerThread(() -> {
			assertEquals("knows", threadedE.label());
		});
	}

	@Test
	public void canCommitInThreadedTx() {
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex("name", "Martin");
		Vertex v2 = graph.addVertex("name", "John");
		v1.addEdge("knows", v2);
		graph.tx().commit();
		// open the threaded transaction
		ChronoGraph threadedTx = graph.tx().createThreadedTx();
		assertNotNull(threadedTx);
		// try to find the elements
		Vertex threadedV1 = Iterators.getOnlyElement(threadedTx.vertices(v1));
		Vertex threadedV2 = Iterators.getOnlyElement(threadedTx.vertices(v2));
		this.executeSynchronouslyInWorkerThread(() -> {
			threadedV1.property("age", 26);
			threadedV2.property("age", 18);
			threadedTx.tx().commit();
		});
		// close the threaded transaction
		threadedTx.close();
		// open a new (regular) transaction on the graph
		graph.tx().reset();
		// make sure that the changes have been applied
		assertEquals(26, (int) v1.value("age"));
		assertEquals(18, (int) v2.value("age"));
	}

	@Test
	public void elementsFromThreadedTxBecomeUnusableOnceThreadedTxIsClosed() {
		ChronoGraph graph = this.getGraph();
		Vertex v1 = graph.addVertex("name", "Martin");
		Vertex v2 = graph.addVertex("name", "John");
		v1.addEdge("knows", v2);
		graph.tx().commit();
		// open the threaded transaction
		ChronoGraph threadedTx = graph.tx().createThreadedTx();
		assertNotNull(threadedTx);
		// try to find the elements
		Vertex threadedV1 = Iterators.getOnlyElement(threadedTx.vertices(v1));
		Vertex threadedV2 = Iterators.getOnlyElement(threadedTx.vertices(v2));
		// close the threaded tx
		threadedTx.close();
		// make sure that the elements are no longer accessible
		try {
			threadedV1.value("name");
			threadedV2.value("name");
			fail("Managed to access a graph element from a threaded transaction after it was closed!");
		} catch (IllegalStateException expected) {
			// pass
		}
	}

	@Test
	public void shouldNotBeAbleToOpenTransactionsFromThreadedTransactionGraph() {
		ChronoGraph graph = this.getGraph();
		ChronoGraph txGraph = graph.tx().createThreadedTx();
		try {
			txGraph.tx().createThreadedTx();
			fail("Managed to open a transaction from within a threaded transaction graph!");
		} catch (UnsupportedOperationException expected) {
			// pass
		}
	}

	@Test
	// disable auto-tx for this test to make sure that only the threaded transaction is really open
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_AUTO_OPEN, value = "false")
	public void canUseIndexManagerFromThreadedTransactionGraph() {
		ChronoGraph graph = this.getGraph();
		graph.getIndexManager().create().stringIndex().onVertexProperty("name").build();
		// work with a threaded graph, add some data
		ChronoGraph tGraph = graph.tx().createThreadedTx();
		tGraph.addVertex("name", "Martin");
		tGraph.addVertex("name", "John");
		// try to run a query in "transient" state mode, i.e. without persistence
		Set<Vertex> set = tGraph.find().vertices().where("name").isEqualToIgnoreCase("martin").toSet();
		assertEquals(1, set.size());
		assertEquals("Martin", Iterables.getOnlyElement(set).value("name"));
		// commit the threaded tx
		tGraph.tx().commit();
		tGraph = null;
		// open a new transaction graph and run the query again
		ChronoGraph tGraph2 = graph.tx().createThreadedTx();
		Set<Vertex> set2 = tGraph2.find().vertices().where("name").isEqualToIgnoreCase("martin").toSet();
		assertEquals(1, set2.size());
		assertEquals("Martin", Iterables.getOnlyElement(set2).value("name"));
	}

	@Test
	public void threadBoundTransactionsDoNotInterfereWithThreadedTransactions() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("hello", "world");
		graph.tx().commit();

		long afterFirstCommit = graph.getNow();

		graph.addVertex("foo", "bar");
		graph.tx().commit();

		long afterSecondCommit = graph.getNow();

		// open a thread-bound tx on "after first commit"

		graph.tx().open(afterFirstCommit);
		assertEquals(afterFirstCommit, graph.tx().getCurrentTransaction().getTimestamp());

		// open a threaded tx on "now"
		try (ChronoGraph txGraph = graph.tx().createThreadedTx()) {
			// make sure that "now" is correctly set on the threaded tx, even though
			// the graph has a thread-bound tx set to an earlier date
			assertEquals(afterSecondCommit, txGraph.getNow());
		}

	}

	@Test
	public void threadedTransactionsDoNotInterfereWithThreadBoundTransactions() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("hello", "world");
		graph.tx().commit();

		long afterFirstCommit = graph.getNow();

		graph.addVertex("foo", "bar");
		graph.tx().commit();

		long afterSecondCommit = graph.getNow();

		// open a threaded tx on "now"
		try (ChronoGraph txGraph = graph.tx().createThreadedTx()) {
			// make sure that "now" is correctly set on the threaded tx, even though
			// the graph has a thread-bound tx set to an earlier date
			assertEquals(afterSecondCommit, txGraph.getNow());
		}

		// open a thread-bound tx on "after first commit"

		graph.tx().open(afterFirstCommit);
		assertEquals(afterFirstCommit, graph.tx().getCurrentTransaction().getTimestamp());
	}

	@Test
	public void threadedTransactionIsNotTreatedAsCurrentTransaction() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("hello", "world");
		graph.tx().commit();

		long afterFirstCommit = graph.getNow();

		graph.addVertex("foo", "bar");
		graph.tx().commit();

		long afterSecondCommit = graph.getNow();

		try (ChronoGraph txGraph = graph.tx().createThreadedTx(afterFirstCommit)) {
			// make sure that "now" is correctly set on the threaded tx, even though
			// the graph has a thread-bound tx set to an earlier date
			assertEquals(afterSecondCommit, txGraph.getNow());

			assertFalse(graph.tx().isOpen());
		}

		assertFalse(graph.tx().isOpen());
	}

	// =====================================================================================================================
	// HELPER METHODS
	// =====================================================================================================================

	private void executeSynchronouslyInWorkerThread(final Runnable runnable) {
		Thread worker = new Thread(runnable);
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
		assertEquals(0, workerThreadExceptions.size());
	}
}
