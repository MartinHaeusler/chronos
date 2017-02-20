package org.chronos.chronograph.test.dump;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
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

}
