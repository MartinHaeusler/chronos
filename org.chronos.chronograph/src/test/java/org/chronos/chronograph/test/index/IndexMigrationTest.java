package org.chronos.chronograph.test.index;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Collections;

import org.apache.commons.io.IOUtils;
import org.chronos.chronodb.internal.util.ChronosFileUtils;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.index.IChronoGraphVertexIndex;
import org.chronos.chronograph.internal.impl.index.ChronoGraphVertexIndex;
import org.chronos.chronograph.test.base.ChronoGraphUnitTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;

@Category(IntegrationTest.class)
@SuppressWarnings("deprecation")
public class IndexMigrationTest extends ChronoGraphUnitTest {

	private static final String RESOURCE_PATH = "org/chronos/chronograph/dump/IndexingGraphDump.zip";

	@Test
	public void canLoadGraphWithOldIndexers() throws Exception {
		InputStream stream = this.getClass().getClassLoader().getResourceAsStream(RESOURCE_PATH);
		assertNotNull("Test resource file not available!", stream);
		File graphZipFile = new File(this.getTestDirectory(), "archive.zip");
		graphZipFile.createNewFile();
		try (FileOutputStream fos = new FileOutputStream(graphZipFile)) {
			IOUtils.copy(stream, fos);
		}
		assertNotNull(graphZipFile);
		assertTrue(graphZipFile.exists());
		assertTrue(graphZipFile.isFile());
		// extract the file
		ChronosFileUtils.extractZipFile(graphZipFile, this.getTestDirectory());
		File dbFile = new File(this.getTestDirectory(), "GraphDump/dump.chronos");
		assertTrue(dbFile.exists());
		assertTrue(dbFile.isFile());
		// open the database
		ChronoGraph g = ChronoGraph.FACTORY.create().chunkDbGraph(dbFile).build();
		// check the contents of the database
		assertEquals(1, g.getIndexManager().getAllIndices().size());
		assertEquals(Collections.singleton("name"), g.getIndexManager().getIndexedVertexPropertyNames());
		assertEquals(1, g.traversal().V().has("name", "John").toSet().size());
		ChronoGraphIndex index = Iterables.getOnlyElement(g.getIndexManager().getAllIndices());
		assertTrue(index instanceof IChronoGraphVertexIndex);
		// it should be migrated away from the deprecated old class
		assertFalse(index instanceof ChronoGraphVertexIndex);

	}
}
