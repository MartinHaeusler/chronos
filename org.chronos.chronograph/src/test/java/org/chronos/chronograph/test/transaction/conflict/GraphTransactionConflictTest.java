package org.chronos.chronograph.test.transaction.conflict;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class GraphTransactionConflictTest extends AllChronoGraphBackendsTest {

    @Test
    public void canMergeNewVertices() {
        ChronoGraph g = this.getGraph();
        ChronoGraph g1 = g.tx().createThreadedTx();
        ChronoGraph g2 = g.tx().createThreadedTx();

        g1.addVertex("name", "John");
        g1.tx().commit();

        g2.addVertex("name", "Jane");
        g2.tx().commit();

        Set<Vertex> vertices = Sets.newHashSet(g.vertices());
        assertThat(vertices.size(), is(2));
        Vertex vJohn = vertices.stream().filter(v -> v.value("name").equals("John")).findFirst().orElse(null);
        Vertex vJane = vertices.stream().filter(v -> v.value("name").equals("Jane")).findFirst().orElse(null);
        assertNotNull(vJohn);
        assertNotNull(vJane);
    }

    @Test
    public void canMergeAdditionalEdgesFromStore() {
        ChronoGraph g = this.getGraph();
        g.addVertex("name", "John");
        g.tx().commit();

        ChronoGraph g1 = g.tx().createThreadedTx();
        ChronoGraph g2 = g.tx().createThreadedTx();

        { // transaction 1
            Vertex vJane = g1.addVertex("name", "Jane");
            Vertex vJohn = g1.traversal().V().has("name", "John").next();
            assertNotNull(vJohn);
            vJohn.addEdge("marriedTo", vJane);
            g1.tx().commit();
        }

        { // transaction 2
            Vertex vJack = g2.addVertex("name", "Jack");
            Vertex vJohn = g2.traversal().V().has("name", "John").next();
            assertNotNull(vJohn);
            vJohn.addEdge("friend", vJack);
            g2.tx().commit();
        }

        // now we should have BOTH Jane and Jack in the graph
        Vertex vJohn = g.traversal().V().has("name", "John").next();
        assertNotNull(vJohn);
        assertThat(Iterators.size(vJohn.edges(Direction.OUT)), is(2));
        Vertex vJack = Iterators.getOnlyElement(vJohn.vertices(Direction.OUT, "friend"));
        assertThat(vJack.value("name"), is("Jack"));
        Vertex vJane = Iterators.getOnlyElement(vJohn.vertices(Direction.OUT, "marriedTo"));
        assertThat(vJane.value("name"), is("Jane"));
    }

    @Test
    public void canMergeRemovedEdgesFromStore() {
        ChronoGraph g = this.getGraph();
        { // initial commit
            Vertex vJohn = g.addVertex("name", "John");
            Vertex vJane = g.addVertex("name", "Jane");
            vJohn.addEdge("marriedTo", vJane);
            g.tx().commit();
        }

        ChronoGraph g1 = g.tx().createThreadedTx();
        ChronoGraph g2 = g.tx().createThreadedTx();

        { // transaction 1
            Edge edge = g1.traversal().E().hasLabel("marriedTo").next();
            edge.remove();
            g1.tx().commit();
        }

        { // transaction 2
            Vertex vJohn = g2.traversal().V().has("name", "John").next();
            Vertex vJack = g2.addVertex("name", "Jack");
            vJohn.addEdge("knows", vJack);
            g2.tx().commit();
        }

        // the "marriedTo" edge should be gone
        Edge marriedToEdge = Iterators.getOnlyElement(g.traversal().E().hasLabel("marriedTo"), null);
        assertNull(marriedToEdge);
        // the "marriedTo" edge should no longer be listed in vJohn or vJane
        Vertex vJohn = g.traversal().V().has("name", "John").next();
        Vertex vJane = g.traversal().V().has("name", "Jane").next();
        assertThat(vJohn.vertices(Direction.OUT, "marriedTo").hasNext(), is(false));
        assertThat(vJohn.edges(Direction.OUT, "marriedTo").hasNext(), is(false));
        assertThat(vJane.vertices(Direction.IN, "marriedTo").hasNext(), is(false));
        assertThat(vJane.edges(Direction.IN, "marriedTo").hasNext(), is(false));

        // the "knows" edge should exist
        Vertex vJack = Iterators.getOnlyElement(g.traversal().V().has("name", "Jack"));
        assertThat(Iterators.getOnlyElement(vJohn.vertices(Direction.OUT, "knows")), is(vJack));
        assertThat(Iterators.getOnlyElement(vJohn.edges(Direction.OUT, "knows")).inVertex(), is(vJack));
    }

    @Test
    public void canMergeEdgeProperties() {
        ChronoGraph g = this.getGraph();
        { // initial commit
            Vertex vJohn = g.addVertex("name", "John");
            Vertex vJane = g.addVertex("name", "Jane");
            vJohn.addEdge("marriedTo", vJane);
            g.tx().commit();
        }

        ChronoGraph g1 = g.tx().createThreadedTx();
        ChronoGraph g2 = g.tx().createThreadedTx();

        { // transaction 1
            Edge edge = g1.traversal().E().next();
            edge.property("since", 2003);
            edge.property("foo", "bar");
            g1.tx().commit();
        }

        { // transaction 2
            Edge edge = g2.traversal().E().next();
            edge.property("foo", "baz");
            edge.property("hello", "world");
            g2.tx().commit();
        }

        // now, the edge should have the following property values
        Edge edge = g.traversal().E().next();
        assertThat(edge.value("foo"), is("baz"));
        assertThat(edge.value("hello"), is("world"));
        assertThat(edge.value("since"), is(2003));
    }

    @Test
    public void canMergeVertexProperties() {
        ChronoGraph g = this.getGraph();
        { // initial commit
            g.addVertex("name", "John");
            g.tx().commit();
        }

        ChronoGraph g1 = g.tx().createThreadedTx();
        ChronoGraph g2 = g.tx().createThreadedTx();

        { // transaction 1
            Vertex vJohn = g1.traversal().V().has("name", "John").next();
            vJohn.property("age", 51);
            vJohn.property("hello", "world");
            g1.tx().commit();
        }

        { // transaction 2
            Vertex vJohn = g2.traversal().V().has("name", "John").next();
            vJohn.property("hello", "foo");
            vJohn.property("test", "me");
            g2.tx().commit();
        }

        // now the vertex should have the following property values
        Vertex vertex = g.traversal().V().next();
        assertThat(vertex.value("age"), is(51));
        assertThat(vertex.value("hello"), is("foo"));
        assertThat(vertex.value("test"), is("me"));
    }

    @Test
    public void updatesOnExistingVertexAreIgnoredIfVertexWasDeleted() {
        ChronoGraph g = this.getGraph();
        { // initial commit
            g.addVertex("name", "John");
            g.tx().commit();
        }

        ChronoGraph g1 = g.tx().createThreadedTx();
        ChronoGraph g2 = g.tx().createThreadedTx();

        { // transaction 1
            Vertex vJohn = g1.traversal().V().has("name", "John").next();
            vJohn.remove();
            g1.tx().commit();
        }

        { // transaction 2
            Vertex vJohn = g2.traversal().V().has("name", "John").next();
            vJohn.property("hello", "world");
            g2.tx().commit();
        }

        // now the vertex should be gone, and the second update should have been discarded.
        Iterator<Vertex> iterator = g.traversal().V().has("name", "John");
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void updatesOnExistingEdgesAreIgnoredIfEdgeWasDeleted() {
        ChronoGraph g = this.getGraph();
        { // initial commit
            Vertex vJohn = g.addVertex("name", "John");
            Vertex vJane = g.addVertex("name", "Jane");
            vJohn.addEdge("marriedTo", vJane);
            g.tx().commit();
        }

        ChronoGraph g1 = g.tx().createThreadedTx();
        ChronoGraph g2 = g.tx().createThreadedTx();

        { // transaction 1
            Edge edge = g1.traversal().E().next();
            edge.remove();
            g1.tx().commit();
        }

        { // transaction 2
            Edge edge = g2.traversal().E().next();
            edge.property("hello", "world");
            g2.tx().commit();
        }

        // now, the edge should have the following property values
        Iterator<Edge> iterator = g.traversal().E();
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void canMergeConflictingGraphVariables() {
        ChronoGraph g = this.getGraph();
        { // initial commit
            g.variables().set("hello", "world");
            g.variables().set("foo", "bar");
            g.tx().commit();
        }

        ChronoGraph g1 = g.tx().createThreadedTx();
        ChronoGraph g2 = g.tx().createThreadedTx();

        g1.variables().set("hello", "john");
        g1.variables().remove("foo");
        g1.variables().set("john", "doe");
        g1.tx().commit();

        g2.variables().set("hello", "jack");
        g2.variables().set("foo", "baz");
        g2.variables().set("jane", "doe");
        g2.tx().commit();

        assertThat(g.variables().get("hello").orElse(null), is("jack"));
        assertThat(g.variables().get("foo").orElse(null), is("baz"));
        assertThat(g.variables().get("john").orElse(null), is("doe"));
        assertThat(g.variables().get("jane").orElse(null), is("doe"));
    }

}
