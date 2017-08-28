package org.chronos.chronograph.test.query;

import static org.junit.Assert.*;

import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;

@Category(IntegrationTest.class)
public class GraphQueryTest extends AllChronoGraphBackendsTest {

	@Test
	public void basicQuerySyntaxTest() {
		ChronoGraph g = this.getGraph();
		Set<Vertex> set = g.find().vertices().where("name").containsIgnoreCase("EVA").and().where("kind")
				.isEqualTo("entity").toSet();
		assertNotNull(set);
		assertTrue(set.isEmpty());
	}

	@Test
	public void queryToGremlinSyntaxTest() {
		ChronoGraph g = this.getGraph();
		Set<Edge> set = g.find().vertices().where("name").containsIgnoreCase("EVA").toTraversal().outE().toSet();
		assertNotNull(set);
		assertTrue(set.isEmpty());
	}

	@Test
	public void simpleVertexQueriesOnPersistentStateWithoutIndexWork() {
		this.performSimpleVertexQueries(false, false, true);
	}

	@Test
	public void simpleVertexQueriesOnTransientStateWithoutIndexWork() {
		this.performSimpleVertexQueries(false, false, false);
	}

	@Test
	public void simpleVertexQueriesOnPersistentStateWithIndexWork() {
		this.performSimpleVertexQueries(true, true, true);
	}

	@Test
	public void simpleVertexQueriesOnTransientStateWithIndexWork() {
		this.performSimpleVertexQueries(true, true, false);
	}

	@Test
	public void gremlinQueriesOnPersistentStateWithoutIndexWork() {
		this.performVertexToGremlinQueries(false, false, true);
	}

	@Test
	public void gremlinQueriesOnTransientStateWithoutIndexWork() {
		this.performVertexToGremlinQueries(false, false, false);
	}

	@Test
	public void gremlinQueriesOnPersistentStateWithIndexWork() {
		this.performVertexToGremlinQueries(true, true, true);
	}

	@Test
	public void gremlinQueriesOnTransientStateWithIndexWork() {
		this.performVertexToGremlinQueries(true, true, false);
	}

	@Test
	public void canQueryNonStandardObjectTypeInHasClause() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("name", new FullName("John", "Doe"));
		graph.addVertex("name", new FullName("Jane", "Doe"));
		graph.tx().commit();

		assertEquals(1, graph.traversal().V().has("name", new FullName("John", "Doe")).toSet().size());
	}

	@Test
	public void canFindStringWithTrailingWhitespace() {
		ChronoGraph graph = this.getGraph();
		graph.getIndexManager().create().stringIndex().onVertexProperty("name").build();
		graph.addVertex("name", "John ");
		graph.addVertex("name", "John");
		graph.tx().commit();

		assertEquals("John ",
				Iterables.getOnlyElement(graph.traversal().V().has("name", "John ").toSet()).value("name"));
	}

	@Test
	public void canFindStringWithLeadingWhitespace() {
		ChronoGraph graph = this.getGraph();
		graph.getIndexManager().create().stringIndex().onVertexProperty("name").build();
		graph.addVertex("name", " John");
		graph.addVertex("name", "John");
		graph.tx().commit();

		assertEquals(" John",
				Iterables.getOnlyElement(graph.traversal().V().has("name", " John").toSet()).value("name"));
	}

	// =====================================================================================================================
	// HELPER METHODS
	// =====================================================================================================================

	private void performSimpleVertexQueries(final boolean indexName, final boolean indexAge,
			final boolean performCommit) {
		ChronoGraph g = this.getGraph();
		if (indexName) {
			g.getIndexManager().create().stringIndex().onVertexProperty("name").build();
			g.tx().commit();
		}
		if (indexAge) {
			g.getIndexManager().create().stringIndex().onVertexProperty("age").build();
			g.tx().commit();
		}
		Vertex vMartin = g.addVertex("name", "Martin", "age", 26);
		Vertex vJohn = g.addVertex("name", "John", "age", 19);
		Vertex vMaria = g.addVertex("name", "Maria", "age", 35);

		if (performCommit) {
			g.tx().commit();
		}

		Set<Vertex> set;

		set = g.find().vertices().where("name").contains("n").toSet(); // Marti[n] and Joh[n]
		assertNotNull(set);
		assertEquals(2, set.size());
		assertTrue(set.contains(vMartin));
		assertTrue(set.contains(vJohn));

		set = g.find().vertices().where("name").containsIgnoreCase("N").toSet(); // Marti[n] and Joh[n]
		assertNotNull(set);
		assertEquals(2, set.size());
		assertTrue(set.contains(vMartin));
		assertTrue(set.contains(vJohn));

		set = g.find().vertices().where("name").contains("N").toSet(); // no capital 'N' to be found in the test data
		assertNotNull(set);
		assertEquals(0, set.size());

		set = g.find().vertices().where("name").startsWith("Mar").toSet(); // [Mar]tin and [Mar]ia
		assertNotNull(set);
		assertEquals(2, set.size());
		assertTrue(set.contains(vMartin));
		assertTrue(set.contains(vMaria));

		set = g.find().vertices().where("name").startsWithIgnoreCase("mar").toSet(); // [Mar]tin and [Mar]ia
		assertNotNull(set);
		assertEquals(2, set.size());
		assertTrue(set.contains(vMartin));
		assertTrue(set.contains(vMaria));

		set = g.find().vertices().where("name").notContains("t").toSet(); // all except Mar[t]in
		assertNotNull(set);
		assertEquals(2, set.size());
		assertTrue(set.contains(vJohn));
		assertTrue(set.contains(vMaria));

		set = g.find().vertices().where("name").matchesRegex(".*r.*i.*").toSet(); // Ma[r]t[i]n and Ma[r][i]a
		assertNotNull(set);
		assertEquals(2, set.size());
		assertTrue(set.contains(vMartin));
		assertTrue(set.contains(vMaria));

		set = g.find().vertices().where("name").matchesRegex("(?i).*R.*I.*").toSet(); // Ma[r]t[i]n and Ma[r][i]a
		assertNotNull(set);
		assertEquals(2, set.size());
		assertTrue(set.contains(vMartin));
		assertTrue(set.contains(vMaria));
	}

	private void performVertexToGremlinQueries(final boolean indexName, final boolean indexAge,
			final boolean performCommit) {
		ChronoGraph g = this.getGraph();
		if (indexName) {
			g.getIndexManager().create().stringIndex().onVertexProperty("name").build();
			g.tx().commit();
		}
		if (indexAge) {
			g.getIndexManager().create().stringIndex().onVertexProperty("age").build();
			g.tx().commit();
		}
		Vertex vMartin = g.addVertex("name", "Martin", "age", 26);
		Vertex vJohn = g.addVertex("name", "John", "age", 19);
		Vertex vMaria = g.addVertex("name", "Maria", "age", 35);

		vMartin.addEdge("knows", vJohn);
		vJohn.addEdge("knows", vMaria);
		vMaria.addEdge("knows", vMartin);

		if (performCommit) {
			g.tx().commit();
		}
		Set<Vertex> set;
		// - The first query (name ends with 'N') delivers Martin and John.
		// - Traversing the 'knows' edge on both Martin and John leads to:
		// -- John (Martin-knows->John)
		// -- Maria (John-knows->Maria)
		set = g.find().vertices().where("name").endsWithIgnoreCase("N").toTraversal().out("knows").toSet();
		assertNotNull(set);
		assertEquals(2, set.size());
		assertTrue(set.contains(vJohn));
		assertTrue(set.contains(vMaria));
	}

	private static class FullName {

		private String firstName;
		private String lastName;

		@SuppressWarnings("unused")
		protected FullName() {
			// (de-)serialization constructor
		}

		public FullName(final String firstName, final String lastName) {
			this.firstName = firstName;
			this.lastName = lastName;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (this.firstName == null ? 0 : this.firstName.hashCode());
			result = prime * result + (this.lastName == null ? 0 : this.lastName.hashCode());
			return result;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (this.getClass() != obj.getClass()) {
				return false;
			}
			FullName other = (FullName) obj;
			if (this.firstName == null) {
				if (other.firstName != null) {
					return false;
				}
			} else if (!this.firstName.equals(other.firstName)) {
				return false;
			}
			if (this.lastName == null) {
				if (other.lastName != null) {
					return false;
				}
			} else if (!this.lastName.equals(other.lastName)) {
				return false;
			}
			return true;
		}

	}

}
