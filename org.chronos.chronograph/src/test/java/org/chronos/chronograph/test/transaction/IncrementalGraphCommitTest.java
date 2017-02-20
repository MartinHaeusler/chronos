package org.chronos.chronograph.test.transaction;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitException;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class IncrementalGraphCommitTest extends AllChronoGraphBackendsTest {

	@Test
	public void canCommitDirectlyAfterIncrementalCommit() {
		ChronoGraph g = this.getGraph();
		try {
			g.addVertex("name", "one");
			g.addVertex("name", "two");
			g.addVertex("name", "three");
			g.tx().commitIncremental();

			g.addVertex("name", "four");
			g.addVertex("name", "five");
			g.addVertex("name", "six");
			g.tx().commitIncremental();
			g.tx().commit();
		} finally {
			g.tx().rollback();
		}
		// assert that the data was written correctly
		assertEquals(6, Iterators.size(g.vertices()));
	}

	@Test
	public void canCommitIncrementally() {
		ChronoGraph g = this.getGraph();
		try {
			g.addVertex("name", "one");
			g.addVertex("name", "two");
			g.addVertex("name", "three");
			g.tx().commitIncremental();

			g.addVertex("name", "four");
			g.addVertex("name", "five");
			g.addVertex("name", "six");
			g.tx().commitIncremental();

			g.addVertex("name", "seven");
			g.addVertex("name", "eight");
			g.addVertex("name", "nine");
			g.tx().commit();
		} finally {
			g.tx().rollback();
		}
		assertEquals(9, Iterators.size(g.vertices()));
	}

	@Test
	public void incrementalCommitTransactionCanReadItsOwnModifications() {
		ChronoGraph g = this.getGraph();
		try {
			g.addVertex("name", "one");
			g.addVertex("name", "two");
			g.addVertex("name", "three");
			g.tx().commitIncremental();
			assertEquals(3, Iterators.size(g.vertices()));
			g.addVertex("name", "four");
			g.addVertex("name", "five");
			g.addVertex("name", "six");
			g.tx().commitIncremental();
			assertEquals(6, Iterators.size(g.vertices()));
			g.addVertex("name", "seven");
			g.addVertex("name", "eight");
			g.addVertex("name", "nine");
			g.tx().commit();
		} finally {
			g.tx().rollback();
		}
		// assert that the data was written correctly
		assertEquals(9, Iterators.size(g.vertices()));
	}

	@Test
	public void incrementalCommitTransactionCanReadItsOwnModificationsInIndexer() {
		ChronoGraph g = this.getGraph();
		g.getIndexManager().createIndex().onVertexProperty("name").build();
		try {
			g.addVertex("name", "one");
			g.addVertex("name", "two");
			g.addVertex("name", "three");
			g.tx().commitIncremental();
			assertEquals(3, Iterators.size(g.vertices()));
			assertEquals(1, g.find().vertices().where("name").isEqualTo("one").toSet().size());
			assertEquals(2, g.find().vertices().where("name").contains("e").toSet().size());

			g.addVertex("name", "four");
			g.addVertex("name", "five");
			g.addVertex("name", "six");
			g.tx().commitIncremental();
			assertEquals(6, Iterators.size(g.vertices()));
			assertEquals(1, g.find().vertices().where("name").isEqualTo("one").toSet().size());
			assertEquals(3, g.find().vertices().where("name").contains("e").toSet().size());

			g.addVertex("name", "seven");
			g.addVertex("name", "eight");
			g.addVertex("name", "nine");
			g.tx().commit();
		} finally {
			g.tx().rollback();
		}
		// assert that the data was written correctly
		assertEquals(9, Iterators.size(g.vertices()));
		assertEquals(1, g.find().vertices().where("name").isEqualTo("one").toSet().size());
		assertEquals(6, g.find().vertices().where("name").contains("e").toSet().size());
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
	public void incrementalCommitTransactionCanReadItsOwnModificationsInIndexerWithQueryCaching() {
		ChronoGraph g = this.getGraph();
		g.getIndexManager().createIndex().onVertexProperty("name").build();
		try {
			g.addVertex("name", "one");
			g.addVertex("name", "two");
			g.addVertex("name", "three");
			g.tx().commitIncremental();
			assertEquals(3, Iterators.size(g.vertices()));
			assertEquals(1, g.find().vertices().where("name").isEqualTo("one").toSet().size());
			assertEquals(2, g.find().vertices().where("name").contains("e").toSet().size());

			g.addVertex("name", "four");
			g.addVertex("name", "five");
			g.addVertex("name", "six");
			g.tx().commitIncremental();
			assertEquals(6, Iterators.size(g.vertices()));
			assertEquals(1, g.find().vertices().where("name").isEqualTo("one").toSet().size());
			assertEquals(3, g.find().vertices().where("name").contains("e").toSet().size());

			g.addVertex("name", "seven");
			g.addVertex("name", "eight");
			g.addVertex("name", "nine");
			g.tx().commit();
		} finally {
			g.tx().rollback();
		}
		// assert that the data was written correctly
		assertEquals(9, Iterators.size(g.vertices()));
		assertEquals(1, g.find().vertices().where("name").isEqualTo("one").toSet().size());
		assertEquals(6, g.find().vertices().where("name").contains("e").toSet().size());
	}

	@Test
	public void cannotCommitOnOtherTransactionWhileIncrementalCommitProcessIsRunning() {
		ChronoGraph g = this.getGraph();
		ChronoGraph tx = g.tx().createThreadedTx();
		try {
			tx.addVertex("name", "one");
			tx.addVertex("name", "two");
			tx.addVertex("name", "three");
			tx.tx().commitIncremental();
			assertEquals(3, Iterators.size(tx.vertices()));
			tx.addVertex("name", "four");
			tx.addVertex("name", "five");
			tx.addVertex("name", "six");
			tx.tx().commitIncremental();

			// simulate a second transaction
			ChronoGraph tx2 = g.tx().createThreadedTx();
			tx2.addVertex("name", "thirteen");
			try {
				tx2.tx().commit();
				fail("Managed to commit on other transaction while incremental commit is active!");
			} catch (ChronoDBCommitException expected) {
				// pass
			}

			// continue with the incremental commit

			assertEquals(6, Iterators.size(tx.vertices()));
			tx.addVertex("name", "seven");
			tx.addVertex("name", "eight");
			tx.addVertex("name", "nine");
			tx.tx().commit();
		} finally {
			tx.tx().rollback();
		}
		// assert that the data was written correctly
		assertEquals(9, Iterators.size(g.vertices()));
	}

	@Test
	public void incrementalCommitsAppearAsSingleCommitInHistory() {
		ChronoGraph g = this.getGraph();
		try {
			g.addVertex("name", "one");
			g.addVertex("name", "two");
			g.addVertex("name", "three");
			Vertex alpha = g.addVertex("name", "alpha", "value", 100);
			g.tx().commitIncremental();

			g.addVertex("name", "four");
			g.addVertex("name", "five");
			g.addVertex("name", "six");
			// update alpha
			alpha.property("value", 200);
			g.tx().commitIncremental();

			g.addVertex("name", "seven");
			g.addVertex("name", "eight");
			g.addVertex("name", "nine");
			// update alpha
			alpha.property("value", 300);
			g.tx().commit();
		} finally {
			g.tx().rollback();
		}
		// assert that the data was written correctly
		assertEquals(10, Iterators.size(g.vertices()));
		// find the alpha vertex
		Vertex alpha = g.find().vertices().where("name").isEqualTo("alpha").toSet().iterator().next();
		// assert that alpha was written only once in the versioning history
		Iterator<Long> historyOfAlpha = g.getVertexHistory(alpha);
		assertEquals(1, Iterators.size(historyOfAlpha));
		// assert that all keys have the same timestamp
		Set<Long> timestamps = Sets.newHashSet();
		g.vertices().forEachRemaining(vertex -> {
			Iterator<Long> history = g.getVertexHistory(vertex);
			timestamps.add(Iterators.getOnlyElement(history));
		});
		assertEquals(1, timestamps.size());
	}

	@Test
	public void rollbackDuringIncrementalCommitWorks() {
		ChronoGraph g = this.getGraph();
		try {
			g.addVertex("name", "one");
			g.addVertex("name", "two");
			g.addVertex("name", "three");
			g.tx().commitIncremental();

			g.addVertex("name", "four");
			g.addVertex("name", "five");
			g.addVertex("name", "six");
			g.tx().commitIncremental();
			// simulate user error
			throw new RuntimeException("User error");
		} catch (RuntimeException expected) {
		} finally {
			g.tx().rollback();
		}
		// assert that the data was rolled back correctly
		assertEquals(0, Iterators.size(g.vertices()));
	}

	@Test
	public void canCommitRegularlyAfterCompletedIncrementalCommitProcess() {
		ChronoGraph g = this.getGraph();
		try {
			g.addVertex("name", "one");
			g.addVertex("name", "two");
			g.addVertex("name", "three");
			g.tx().commitIncremental();

			g.addVertex("name", "four");
			g.addVertex("name", "five");
			g.addVertex("name", "six");
			g.tx().commitIncremental();

			g.addVertex("name", "seven");
			g.addVertex("name", "eight");
			g.addVertex("name", "nine");
			g.tx().commit();
		} finally {
			g.tx().rollback();
		}
		assertEquals(9, Iterators.size(g.vertices()));

		// can commit on a different transaction
		ChronoGraph tx2 = g.tx().createThreadedTx();
		tx2.addVertex("name", "fourtytwo");
		tx2.tx().commit();

		// can commit on the same transaction
		g.addVertex("name", "fourtyseven");
		g.tx().commit();

		assertEquals(11, Iterators.size(g.vertices()));
	}

	@Test
	public void canCommitRegularlyAfterCanceledIncrementalCommitProcess() {
		ChronoGraph g = this.getGraph();
		try {
			g.addVertex("name", "one");
			g.addVertex("name", "two");
			g.addVertex("name", "three");
			g.tx().commitIncremental();

			g.addVertex("name", "four");
			g.addVertex("name", "five");
			g.addVertex("name", "six");
			g.tx().commitIncremental();

			g.addVertex("name", "seven");
			g.addVertex("name", "eight");
			g.addVertex("name", "nine");
		} finally {
			g.tx().rollback();
		}
		assertEquals(0, Iterators.size(g.vertices()));

		// can commit on a different transaction
		ChronoGraph tx2 = g.tx().createThreadedTx();
		tx2.addVertex("name", "fourtytwo");
		tx2.tx().commit();

		// can commit on the same transaction
		g.addVertex("name", "fourtyseven");
		g.tx().commit();

		assertEquals(2, Iterators.size(g.vertices()));
	}

	@Test
	@SuppressWarnings("unused")
	public void secondaryIndexingIsCorrectDuringIncrementalCommit() {
		ChronoGraph g = this.getGraph();
		g.getIndexManager().createIndex().onVertexProperty("firstName").build();
		g.getIndexManager().createIndex().onVertexProperty("lastName").build();
		g.tx().commit();
		try {
			// add three persons
			Vertex p1 = g.addVertex(T.id, "p1", "firstName", "John", "lastName", "Doe");
			Vertex p2 = g.addVertex(T.id, "p2", "firstName", "John", "lastName", "Smith");
			Vertex p3 = g.addVertex(T.id, "p3", "firstName", "Jane", "lastName", "Doe");

			// perform the incremental commit
			g.tx().commitIncremental();

			// make sure that we can find them
			assertEquals(2, g.find().vertices().where("firstName").isEqualToIgnoreCase("john").count());
			assertEquals(2, g.find().vertices().where("lastName").isEqualToIgnoreCase("doe").count());

			// change Jane's and John's first names
			p3.property("firstName", "Jayne");
			p1.property("firstName", "Jack");

			// perform the incremental commit
			g.tx().commitIncremental();

			// make sure that we can't find John any longer
			assertEquals(1, g.find().vertices().where("firstName").isEqualToIgnoreCase("john").count());
			assertEquals(2, g.find().vertices().where("lastName").isEqualToIgnoreCase("doe").count());

			// change Jack's first name (yet again)
			p1.property("firstName", "Joe");

			// perform the incremental commit
			g.tx().commitIncremental();

			// make sure that we can't find Jack Doe any longer
			assertEquals(0, g.find().vertices().where("firstName").isEqualToIgnoreCase("jack").count());
			// john smith should still be there
			assertEquals(1, g.find().vertices().where("firstName").isEqualToIgnoreCase("john").count());
			// we still have jayne doe and joe doe
			assertEquals(2, g.find().vertices().where("lastName").isEqualToIgnoreCase("doe").count());
			// jayne should be in the first name index as well
			assertEquals(1, g.find().vertices().where("firstName").isEqualToIgnoreCase("jayne").count());

			// delete Joe
			p1.remove();

			// make sure that joe's gone
			assertEquals(0, g.find().vertices().where("firstName").isEqualTo("joe").count());

			// do the full commit
			g.tx().commit();
		} finally {
			g.tx().rollback();
		}

		// in the end, there should be john smith and jayne doe
		assertEquals(1, g.find().vertices().where("firstName").isEqualToIgnoreCase("john").count());
		assertEquals(1, g.find().vertices().where("lastName").isEqualToIgnoreCase("doe").count());
	}
}
