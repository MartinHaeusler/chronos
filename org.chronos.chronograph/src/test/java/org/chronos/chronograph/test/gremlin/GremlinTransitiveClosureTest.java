package org.chronos.chronograph.test.gremlin;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class GremlinTransitiveClosureTest extends AllChronoGraphBackendsTest {

    @Test
    public void runTransitiveClosureTestOnAcyclicGraph() {
        ChronoGraph g = this.getGraph();
        Vertex vApp = g.addVertex("name", "MyApp", "kind", "Application");
        Vertex vVM1 = g.addVertex("name", "VM1", "kind", "VirtualMachine");
        Vertex vVM2 = g.addVertex("name", "VM2", "kind", "VirtualMachine");
        Vertex vVM3 = g.addVertex("name", "VM3", "kind", "VirtualMachine");
        Vertex vVM4 = g.addVertex("name", "VM4", "kind", "VirtualMachine");
        Vertex vVM5 = g.addVertex("name", "VM5", "kind", "VirtualMachine");
        Vertex vVM6 = g.addVertex("name", "VM6", "kind", "VirtualMachine");
        Vertex vVM7 = g.addVertex("name", "VM7", "kind", "VirtualMachine");
        Vertex vVM8 = g.addVertex("name", "VM8", "kind", "VirtualMachine");
        Vertex vPM1 = g.addVertex("name", "PM1", "kind", "PhysicalMachine");
        Vertex vPM2 = g.addVertex("name", "PM2", "kind", "PhysicalMachine");
        Vertex vPM3 = g.addVertex("name", "PM3", "kind", "PhysicalMachine");
        Vertex vPM4 = g.addVertex("name", "PM4", "kind", "PhysicalMachine");
        vApp.addEdge("runsOn", vVM1);
        vVM1.addEdge("runsOn", vVM2);
        vVM2.addEdge("runsOn", vVM3);
        vVM2.addEdge("runsOn", vVM4);
        vVM3.addEdge("runsOn", vVM5);
        vVM3.addEdge("runsOn", vVM6);
        vVM4.addEdge("runsOn", vVM7);
        vVM4.addEdge("runsOn", vVM8);
        vVM5.addEdge("runsOn", vPM1);
        vVM6.addEdge("runsOn", vPM2);
        vVM7.addEdge("runsOn", vPM3);
        vVM8.addEdge("runsOn", vPM4);
        g.tx().commit();

        Set<Vertex> physicalMachines = g.traversal()
            .V()
            .has("name", "MyApp").repeat(out("runsOn")).emit(t -> {
                Object value = t.get().property("kind").orElse(null);
                return Objects.equal(value, "PhysicalMachine");
            })
            .until(t -> false)
            .toSet();

        for (Vertex v : physicalMachines) {
            System.out.println(v.id() + ": " + v.value("name") + " (" + v.value("kind") + ")");
        }
        assertEquals(Sets.newHashSet(vPM1, vPM2, vPM3, vPM4), physicalMachines);
    }


    @Test
    public void runTransitiveClosureTestOnCyclicGraph() {
        ChronoGraph g = this.getGraph();
        // these four vertices form a circle
        Vertex v1 = g.addVertex("name", "V1");
        Vertex v2 = g.addVertex("name", "V2");
        Vertex v3 = g.addVertex("name", "V3");
        Vertex v4 = g.addVertex("name", "V4");
        v1.addEdge("connect", v2);
        v2.addEdge("connect", v3);
        v3.addEdge("connect", v4);
        v4.addEdge("connect", v1);

        // to each vertex in the circle, we add a vertex to form a 2-step-loop
        Vertex v5 = g.addVertex("name", "V5");
        Vertex v6 = g.addVertex("name", "V6");
        Vertex v7 = g.addVertex("name", "V7");
        Vertex v8 = g.addVertex("name", "V8");
        v1.addEdge("connect", v5);
        v5.addEdge("connect", v1);
        v2.addEdge("connect", v6);
        v6.addEdge("connect", v2);
        v3.addEdge("connect", v7);
        v7.addEdge("connect", v3);
        v4.addEdge("connect", v8);
        v8.addEdge("connect", v4);

        g.tx().commit();

        // run the query
        Collection<Path> paths = g.traversal().V(v1).repeat(out("connect").simplePath()).emit().until(t -> false).path().toList();

        // this block is just for debugging, it prints the encountered paths.
        paths.forEach(path -> {
            String separator = "";
            for (Object element : path) {
                Vertex v = (Vertex) element;
                System.out.print(separator + v.value("name"));
                separator = " -> ";
            }
            System.out.println();
        });

        List<Vertex> closureVertices = paths.stream().map(Path::head).map(e -> (Vertex) e).collect(Collectors.toList());
        // we have 8 vertices in total. The source of the closure is not part of the closure,
        // therefore we should have 7 vertices in the closure.
        assertEquals(7, closureVertices.size());

        assertTrue(closureVertices.contains(v2));
        assertTrue(closureVertices.contains(v3));
        assertTrue(closureVertices.contains(v4));
        assertTrue(closureVertices.contains(v5));
        assertTrue(closureVertices.contains(v6));
        assertTrue(closureVertices.contains(v7));
    }
}
