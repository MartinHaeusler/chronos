package org.chronos.chronograph.test.index;

import static org.junit.Assert.*;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class IndexingTest extends AllChronoGraphBackendsTest {

	@Test
	public void indexCreationWorks() {
		ChronoGraph g = this.getGraph();
		ChronoGraphIndex index = g.getIndexManager().createIndex().onVertexProperty("name").build();
		assertNotNull(index);
		g.tx().commit();

		Vertex v = g.addVertex("name", "Martin");
		g.addVertex("name", "John");
		g.addVertex("name", "Martina");

		g.tx().commit();

		// find by case-insensitive search
		assertEquals(v, Iterables.getOnlyElement(g.traversal().V().has("name", "Martin").toSet()));

	}

	@Test
	public void canDropIndex() {
		ChronoGraph g = this.getGraph();
		ChronoGraphIndex index = g.getIndexManager().createIndex().onVertexProperty("name").build();
		assertNotNull(index);
		g.getIndexManager().reindex(index);

		assertTrue(g.getIndexManager().isVertexPropertyIndexed("name"));

		g.getIndexManager().dropIndex(index);

		assertFalse(g.getIndexManager().isVertexPropertyIndexed("name"));
	}

	@Test
	public void renamingElementWorks() {
		ChronoGraph g = this.getGraph();
		g.getIndexManager().createIndex().onVertexProperty("name").build();

		Vertex v1 = g.addVertex("name", "Test");
		Vertex v2 = g.addVertex("name", "John");
		g.tx().commit();

		// assert that we can find the elements
		assertEquals(v1, Iterables.getOnlyElement(g.traversal().V().has("name", "Test").toSet()));
		assertEquals(v2, Iterables.getOnlyElement(g.traversal().V().has("name", "John").toSet()));

		// rename one element
		v1.property("name", "Hello");

		// assert that we can find the element by its new name
		assertEquals(v1, Iterables.getOnlyElement(g.traversal().V().has("name", "Hello").toSet()));
		// assert that we cannot find the element by its old name anymore
		assertEquals(0, g.traversal().V().has("name", "Test").toSet().size());

		// perform the commit
		g.tx().commit();

		// assert that we can find the element by its new name
		assertEquals(v1, Iterables.getOnlyElement(g.traversal().V().has("name", "Hello").toSet()));
		// assert that we cannot find the element by its old name anymore
		assertEquals(0, g.traversal().V().has("name", "Test").toSet().size());

	}

	@Test
	public void renamingElementInIncrementalCommitWorks() {
		ChronoGraph g = this.getGraph();
		g.getIndexManager().createIndex().onVertexProperty("name").build();

		Vertex v1 = g.addVertex("name", "Test");
		Vertex v2 = g.addVertex("name", "John");
		g.tx().commit();

		// assert that we can find the elements
		assertEquals(v1, Iterables.getOnlyElement(g.traversal().V().has("name", "Test").toSet()));
		assertEquals(v2, Iterables.getOnlyElement(g.traversal().V().has("name", "John").toSet()));

		g.tx().commitIncremental();

		// rename one element
		v1.property("name", "Hello");

		// assert that we can find the element by its new name
		assertEquals(v1, Iterables.getOnlyElement(g.traversal().V().has("name", "Hello").toSet()));
		// assert that we cannot find the element by its old name anymore
		assertEquals(0, g.traversal().V().has("name", "Test").toSet().size());

		g.tx().commitIncremental();

		// perform the commit
		g.tx().commit();

		// assert that we can find the element by its new name
		assertEquals(v1, Iterables.getOnlyElement(g.traversal().V().has("name", "Hello").toSet()));
		// assert that we cannot find the element by its old name anymore
		assertEquals(0, g.traversal().V().has("name", "Test").toSet().size());
	}

	@Test
	@SuppressWarnings("unused")
	public void queryingTransientStateWithIndexWorks() {
		ChronoGraph g = this.getGraph();
		g.getIndexManager().createIndex().onVertexProperty("color").build();
		g.tx().open();
		Vertex v0 = g.addVertex("color", "red");
		Vertex v1 = g.addVertex("color", "green");
		Vertex v2 = g.addVertex("color", "blue");

		assertEquals(Sets.newHashSet(v0, v1), g.find().vertices().where("color").contains("r").toSet());

		g.tx().commit();

		assertEquals(Sets.newHashSet(v0, v1), g.find().vertices().where("color").contains("r").toSet());
	}

	@Test
	@SuppressWarnings("unused")
	public void queryingTransientStateWithoutIndexWorks() {
		ChronoGraph g = this.getGraph();
		g.tx().open();
		Vertex v0 = g.addVertex("color", "red");
		Vertex v1 = g.addVertex("color", "green");
		Vertex v2 = g.addVertex("color", "blue");

		assertEquals(Sets.newHashSet(v0, v1), g.find().vertices().where("color").contains("r").toSet());

		g.tx().commit();

		assertEquals(Sets.newHashSet(v0, v1), g.find().vertices().where("color").contains("r").toSet());
	}

	@Test
	@SuppressWarnings("unused")
	public void indexingOfMultiplicityManyValuesWorks() {
		ChronoGraph g = this.getGraph();
		g.getIndexManager().createIndex().onVertexProperty("colors").build();
		g.tx().open();
		Vertex v0 = g.addVertex("colors", Sets.newHashSet("red", "green", "blue"));
		Vertex v1 = g.addVertex("colors", Sets.newHashSet("red", "black", "white"));
		Vertex v2 = g.addVertex("colors", Sets.newHashSet("yellow", "green", "white"));

		// try to find all white vertices
		assertEquals(Sets.newHashSet(v1, v2), g.find().vertices().where("colors").isEqualToIgnoreCase("white").toSet());

		g.tx().commit();

		// try to find all white vertices
		assertEquals(Sets.newHashSet(v1, v2), g.find().vertices().where("colors").isEqualToIgnoreCase("white").toSet());

	}

	@Test
	public void t_EntityTest1() {
		ChronoGraph g = this.getGraph();
		g.getIndexManager().createIndex().onVertexProperty("Name").build();
		g.getIndexManager().createIndex().onVertexProperty("Kind").build();
		g.tx().open();
		// g.tx().commitIncremental();
		Vertex entity1 = g.addVertex(T.id, "id1", "Name", "name1", "Kind", "entity", "Site", "propertyA", "OLA",
				"Gold");
		Vertex entity2 = g.addVertex(T.id, "id2", "Name", "name2", "Kind", "entity", "Site", "propertyB");
		// g.tx().commitIncremental();
		g.tx().commit();

		// assert that we find the entities
		assertEquals(entity1,
				Iterables.getOnlyElement(g.traversal().V().has("Name", "name1").has("Kind", "entity").toSet()));
		assertEquals(entity2,
				Iterables.getOnlyElement(g.traversal().V().has("Name", "name2").has("Kind", "entity").toSet()));
		g.tx().rollback();

		// perform some modifications on the vertices
		g.tx().open();
		// g.tx().commitIncremental();
		entity1.property("Name", "propertyA");
		entity1.property("Site").remove();
		entity1.property("OLA").remove();
		entity2.property("Name", "propertyB");
		entity2.property("Site").remove();
		entity2.property("OLA").remove();
		// g.tx().commitIncremental();
		g.tx().commit();

		// assert that we find the entities
		assertEquals(entity1,
				Iterables.getOnlyElement(g.traversal().V().has("Name", "propertyA").has("Kind", "entity").toSet()));
		assertEquals(entity2,
				Iterables.getOnlyElement(g.traversal().V().has("Name", "propertyB").has("Kind", "entity").toSet()));

	}
}
