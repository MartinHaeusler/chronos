package org.chronos.chronograph.test.structure;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterators;

@Category(IntegrationTest.class)
public class EdgeSymmetryTest extends AllChronoGraphBackendsTest {

	@Test
	public void canAssignPropertyToEdgeFromBothSides() {
		ChronoGraph g = this.getGraph();
		Vertex v1 = g.addVertex();
		Vertex v2 = g.addVertex();
		v1.addEdge("test", v2);
		g.tx().commit();

		// fetch the edge twice, once from the left and once from the right
		Edge eFromLeft = Iterators.getOnlyElement(v1.edges(Direction.OUT));
		Edge eFromRight = Iterators.getOnlyElement(v2.edges(Direction.IN));
		assertTrue(eFromLeft.equals(eFromRight));
		// the two objects do not necessarily need to be the same (w.r.t. ==), but
		// if we change one of them, the other one should change correspondingly.
		eFromLeft.property("hello", "world");
		assertThat(eFromRight.value("hello"), is("world"));
		eFromRight.property("foo", "bar");
		assertThat(eFromLeft.value("foo"), is("bar"));
		// removing one should remove the other as well
		eFromLeft.remove();
		assertThat(((ChronoEdge) eFromLeft).isRemoved(), is(true));
		assertThat(((ChronoEdge) eFromRight).isRemoved(), is(true));
	}

	@Test
	public void canAssignPropertyToVertexFromBothSides() {
		ChronoGraph g = this.getGraph();
		{ // insert data
			Vertex v1 = g.addVertex(T.id, "v1");
			Vertex v2 = g.addVertex(T.id, "v2");
			Vertex v3 = g.addVertex(T.id, "v3");
			v1.addEdge("test", v2);
			v2.addEdge("test", v3);
		}
		g.tx().commit();

		Vertex v1 = Iterators.getOnlyElement(g.vertices("v1"));
		Vertex v3 = Iterators.getOnlyElement(g.vertices("v3"));
		Edge e1 = Iterators.getOnlyElement(v1.edges(Direction.OUT));
		Edge e2 = Iterators.getOnlyElement(v3.edges(Direction.IN));
		Vertex v2FromLeft = e1.inVertex();
		Vertex v2FromRight = e2.outVertex();
		assertTrue(v2FromLeft.equals(v2FromRight));
		// changing one should change the other
		v2FromLeft.property("hello", "world");
		assertThat(v2FromRight.value("hello"), is("world"));
		v2FromRight.property("foo", "bar");
		assertThat(v2FromLeft.value("foo"), is("bar"));
		// removing one should remove the other
		v2FromLeft.remove();
		assertThat(((ChronoVertex) v2FromRight).isRemoved(), is(true));
	}

}
