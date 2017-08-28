package org.chronos.benchmarks.chronodb.secondaryindexing;

import java.util.Set;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronodb.test.base.AllBackendsTest.DontRunWithBackend;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(PerformanceTest.class)
@DontRunWithBackend({ ChronosBackend.JDBC, ChronosBackend.MAPDB })
public class StringContainsIndexingPerformanceTest extends AllChronoGraphBackendsTest {

	@Test
	public void runBenchmark() {
		ChronoGraph g = this.getGraph();
		g.getIndexManager().create().stringIndex().onVertexProperty("name").build();
		g.getIndexManager().create().stringIndex().onVertexProperty("kind").build();

		System.out.println("Inserting data into graph...");
		long timeBeforeInsert = System.currentTimeMillis();
		for (int i = 0; i < 200_000; i++) {
			Vertex v = g.addVertex("name", UUID.randomUUID().toString());
			if (Math.random() > 0.5) {
				v.property("kind", "one");
			} else {
				v.properties("kind", "two");
			}
		}
		g.tx().commit();
		long timeAfterInsert = System.currentTimeMillis();
		System.out.println("Insertion time: " + (timeAfterInsert - timeBeforeInsert) + "ms.");

		long timeBeforeQuery = System.currentTimeMillis();
		Set<Vertex> vertices = g.find().vertices().where("name").contains("123").and().where("kind").isEqualTo("one")
				.toSet();
		long timeAfterQuery = System.currentTimeMillis();

		System.out.println("Found " + vertices.size() + " vertices in " + (timeAfterQuery - timeBeforeQuery) + "ms.");
	}
}
