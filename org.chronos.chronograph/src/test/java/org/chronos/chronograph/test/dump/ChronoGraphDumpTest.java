package org.chronos.chronograph.test.dump;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.chronos.common.util.ClasspathUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ChronoGraphDumpTest extends AllChronoGraphBackendsTest {

	@Test
	public void canCreateGraphDump() {
		ChronoGraph graph = this.getGraph();
		Vertex vJohn = graph.addVertex("firstname", "John", "lastname", "Doe");
		Vertex vJane = graph.addVertex("firstname", "Jane", "lastname", "Doe");
		Vertex vJack = graph.addVertex("firstname", "Jack", "lastname", "Doe");
		vJohn.addEdge("family", vJack, "kind", "brother");
		vJohn.addEdge("family", vJane, "kind", "married");
		vJane.addEdge("family", vJohn, "kind", "married");
		vJane.addEdge("family", vJack, "kind", "brother-in-law");
		vJack.addEdge("family", vJohn, "kind", "brohter");
		vJack.addEdge("family", vJane, "kind", "sister-in-law");
		graph.tx().commit();

		// create the dump file
		File dumpFile = this.createTestFile("Test.chronodump");

		// write the dump
		graph.writeDump(dumpFile);

		// print the contents of the file (for debugging)
		try {
			String contents = FileUtils.readFileToString(dumpFile);
			System.out.println(contents);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}

		// reinstantiate the DB
		ChronoGraph graph2 = this.reinstantiateGraph();

		// read the contents of the dump file into the graph
		graph2.readDump(dumpFile);

		// make sure that the data is still available
		Vertex vJohn2 = graph2.vertices(vJohn.id()).next();
		Vertex vJane2 = graph2.vertices(vJane.id()).next();
		Vertex vJack2 = graph2.vertices(vJack.id()).next();
		assertNotNull(vJohn2);
		assertNotNull(vJane2);
		assertNotNull(vJack2);
		// john should be married to jane
		assertEquals(vJane2, graph2.traversal().V(vJohn2).outE("family").has("kind", "married").inV().next());
		assertEquals(vJohn2, graph2.traversal().V(vJane2).inE("family").has("kind", "married").outV().next());
		// jane should be married to john
		assertEquals(vJohn2, graph2.traversal().V(vJane2).outE("family").has("kind", "married").inV().next());
		assertEquals(vJane2, graph2.traversal().V(vJohn2).inE("family").has("kind", "married").outV().next());

	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "100")
	public void canReadDumpWithCaching() throws Exception {
		this.canReadDumpTest();
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "false")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "false")
	public void canCreadDumpWithoutCaching() throws Exception {
		this.canReadDumpTest();
	}

	private void canReadDumpTest() throws Exception {
		// get the dump file
		File dumpFile = ClasspathUtils.getResourceAsFile("org/chronos/chronograph/dump/dumpReaderTest.xml");
		assertNotNull(dumpFile);
		assertTrue(dumpFile.exists());
		assertTrue(dumpFile.isFile());

		// load it into the graph
		ChronoGraph graph = this.getGraph();
		graph.readDump(dumpFile);

		// make sure we have a vertex with "kind" equal to "informationModel"
		assertEquals(1, graph.traversal().V().filter(t -> t.get().property("kind").orElse("").equals("informationModel")).toSet().size());

		// make sure that the secondary index agrees as well
		assertEquals(1, graph.traversal().V().has("kind", "informationModel").toSet().size());

		{ // our secondary index should be correct on any timestamp
			List<Long> commitTimestamps = graph.getCommitTimestampsBefore(System.currentTimeMillis() + 1, Integer.MAX_VALUE);
			for (long timestamp : commitTimestamps) {
				try (ChronoGraph txGraph = graph.tx().createThreadedTx(timestamp)) {
					System.out.println("Checking timestamp " + timestamp);
					assertSameResultFromIndexAndGraph(txGraph, "kind", "entity");
					assertSameResultFromIndexAndGraph(txGraph, "kind", "entityClass");
					assertSameResultFromIndexAndGraph(txGraph, "kind", "informationModel");
					assertSameResultFromIndexAndGraph(txGraph, "kind", "associationClass");
					assertSameResultFromIndexAndGraph(txGraph, "kind", "reference");
				}
			}
		}

		// reindexing should not affect the correctness of our result
		graph.getIndexManager().reindexAll();

		// make sure we have a vertex with "kind" equal to "informationModel"
		assertEquals(1, graph.traversal().V().filter(t -> t.get().property("kind").orElse("").equals("informationModel")).toSet().size());

		// make sure that the secondary index agrees as well
		assertEquals(1, graph.traversal().V().has("kind", "informationModel").toSet().size());

		// our secondary index should be correct on any timestamp
		{
			List<Long> commitTimestamps = graph.getCommitTimestampsBefore(System.currentTimeMillis() + 1, Integer.MAX_VALUE);
			for (long timestamp : commitTimestamps) {
				try (ChronoGraph txGraph = graph.tx().createThreadedTx(timestamp)) {
					System.out.println("Checking timestamp " + timestamp);
					assertSameResultFromIndexAndGraph(txGraph, "kind", "entity");
				}
			}
		}
	}

	// =====================================================================================================================
	// HELPER METHODS
	// =====================================================================================================================

	private File createTestFile(final String filename) {
		File testDirectory = this.getTestDirectory();
		File testFile = new File(testDirectory, filename);
		try {
			testFile.createNewFile();
		} catch (IOException ioe) {
			fail(ioe.toString());
		}
		testFile.deleteOnExit();
		return testFile;
	}

	private static Set<String> filterVerticesWithIndex(final ChronoGraph graph, final String property, final String value) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		return graph.traversal().V().has(property, value).toStream().map(v -> (String) v.id()).collect(Collectors.toSet());
	}

	private static Set<String> filterVerticesWithoutIndex(final ChronoGraph graph, final String property, final String value) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		return graph.traversal().V().toStream().filter(v -> v.property(property).isPresent()).filter(v -> v.value(property).equals(value)).map(v -> (String) v.id()).collect(Collectors.toSet());
	}

	private static void assertSameResultFromIndexAndGraph(final ChronoGraph graph, final String property, final String value) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		Set<String> idsFromIndex = filterVerticesWithIndex(graph, property, value);
		Set<String> idsFromGraph = filterVerticesWithoutIndex(graph, property, value);
		assertEquals(idsFromGraph.size(), idsFromIndex.size());
		assertEquals(idsFromGraph, idsFromIndex);
	}
}
