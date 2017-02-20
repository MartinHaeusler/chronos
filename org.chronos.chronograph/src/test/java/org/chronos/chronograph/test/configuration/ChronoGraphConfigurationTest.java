package org.chronos.chronograph.test.configuration;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ChronoGraphConfigurationTest extends AllChronoGraphBackendsTest {

	@Test
	public void chronoGraphConfigurationIsPresent() {
		ChronoGraph graph = this.getGraph();
		assertNotNull(graph.getChronoGraphConfiguration());
	}

	@Test
	public void idExistenceCheckIsEnabledByDefault() {
		ChronoGraph graph = this.getGraph();
		assertTrue(graph.getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled());
	}

	@Test
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
	public void canDisableIdExistenceCheckWithTestAnnotation() {
		ChronoGraph graph = this.getGraph();
		assertFalse(graph.getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled());
	}

	@Test
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
	public void disablingIdExistenceCheckWorks() {
		ChronoGraph graph = this.getGraph();
		assertFalse(graph.getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled());
		// this should NEVER be done in application code, this just demonstrates that the check is indeed disabled.
		// if the check was enabled, this would throw an exception.
		Vertex v1 = graph.addVertex(T.id, "MyAwesomeId");
		Vertex v2 = graph.addVertex(T.id, "MyAwesomeId");
		assertNotNull(v1);
		assertNotNull(v2);
	}

	@Test
	public void enablingIdExistenceCheckThrowsExceptionIfIdIsUsedTwice() {
		ChronoGraph graph = this.getGraph();
		assertTrue(graph.getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled());
		Vertex v1 = graph.addVertex(T.id, "MyAwesomeId");
		assertNotNull(v1);
		try {
			graph.addVertex(T.id, "MyAwesomeId");
			fail("Managed to use the same ID twice!");
		} catch (IllegalArgumentException expected) {
			// pass
		}
	}

	@Test
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_AUTO_OPEN, value = "false")
	public void canDisableAutoStartTransactions() {
		ChronoGraph graph = this.getGraph();
		assertFalse(graph.getChronoGraphConfiguration().isTransactionAutoOpenEnabled());
		try {
			graph.addVertex();
			fail("Managed to add a vertex to a graph while auto-transactions are disabled!");
		} catch (IllegalStateException expected) {
			// pass
		}
		// try in another thread
		GraphAccessTestRunnable runnable = new GraphAccessTestRunnable(graph);
		Thread worker = new Thread(runnable);
		worker.start();
		try {
			worker.join();
		} catch (InterruptedException e) {
		}
		assertFalse(runnable.canAccessGraph());
	}

	@Test
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_AUTO_OPEN, value = "false")
	public void canOpenManualTransactionsWhenAutoStartIsDisabled() {
		ChronoGraph graph = this.getGraph();
		assertFalse(graph.getChronoGraphConfiguration().isTransactionAutoOpenEnabled());
		graph.tx().open();
		try {
			Vertex v = graph.addVertex();
			v.property("name", "Martin");
			graph.tx().commit();
		} finally {
			if (graph.tx().isOpen()) {
				graph.tx().close();
			}
		}
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private static class GraphAccessTestRunnable implements Runnable {

		private final ChronoGraph graph;
		private boolean canAccessGraph;

		public GraphAccessTestRunnable(ChronoGraph graph) {
			checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
			this.graph = graph;
		}

		@Override
		public void run() {
			try {
				this.graph.addVertex();
				this.canAccessGraph = true;
			} catch (IllegalStateException expected) {
				this.canAccessGraph = false;
			} finally {
				if (this.graph.tx().isOpen()) {
					this.graph.tx().rollback();
				}
			}
		}

		public boolean canAccessGraph() {
			return this.canAccessGraph;
		}
	}
}
