package org.chronos.chronodb.test.engine.migration;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.internal.impl.engines.chunkdb.ChunkedChronoDB;
import org.chronos.chronodb.internal.util.ChronosFileUtils;
import org.chronos.common.test.ChronosUnitTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.chronos.common.util.ClasspathUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class Chronos0_5_7_to_0_6_0_MigrationTest extends ChronosUnitTest {

	private static final String TEST_RESOURCE_FILE_NAME = "migrationTestResources/ChronoDB_v0_5_7_BranchTest_ChunkDB.zip";

	@Test
	public void canMigrateBranchFormat() throws IOException {
		// load up the resource file
		File testResourceZipFile = ClasspathUtils.getResourceAsFile(TEST_RESOURCE_FILE_NAME);
		assertNotNull(testResourceZipFile);
		assertTrue(testResourceZipFile.exists());
		// unzip the file to our test directory
		File testDir = this.getTestDirectory();
		// unpack the *.zip file
		ChronosFileUtils.extractZipFile(testResourceZipFile, testDir);
		// we should now have a "test.chronos" file in there
		File chronosFile = new File(testDir, "test.chronos");
		assertTrue(chronosFile.exists());
		assertTrue(chronosFile.isFile());
		// start up the chronos instance
		try (ChronoDB chronoDB = ChronoDB.FACTORY.create().chunkedDatabase(chronosFile).build()) {
			// assert that the properties file for the master branch exists
			File masterBranchProperties = new File(testDir + "/branches/master", ChunkedChronoDB.FILENAME__BRANCH_METADATA_FILE);
			assertTrue(masterBranchProperties.exists());
			assertTrue(masterBranchProperties.isFile());
			// assert that the branches exist
			assertNotNull(chronoDB.getBranchManager().getBranch("TestBranch"));
			assertNotNull(chronoDB.getBranchManager().getBranch("SubBranch"));
			// assert that the data is still in there
			assertEquals("world", chronoDB.tx("TestBranch").get("hello"));
			assertNull(chronoDB.tx("TestBranch").get("foo"));
			assertEquals("bar", chronoDB.tx("SubBranch").get("foo"));
			assertEquals("world", chronoDB.tx("SubBranch").get("hello"));
			// assert that the branch name flag is set
			assertEquals("TestBranch", chronoDB.getBranchManager().getBranch("TestBranch").getDirectoryName());
			assertEquals("SubBranch", chronoDB.getBranchManager().getBranch("SubBranch").getDirectoryName());
		}
	}
}
