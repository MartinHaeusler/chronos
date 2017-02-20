package org.chronos.chronograph.test.transaction;

import static org.junit.Assert.*;

import java.util.Map;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;

@Category(IntegrationTest.class)
public class CommitMetadataTest extends AllChronoGraphBackendsTest {

	@Test
	public void canStoreAndRetrieveCommitMetadata() {
		ChronoGraph graph = this.getGraph();
		// the message we are going to store as metadata; stored as a variable for later comparison
		final String commitMessage = "Committed Hello World!";
		// add some data with our commit message
		graph.addVertex("message", "Hello World!");
		graph.tx().commit(commitMessage);
		// the "now" timestamp is pointing to the last commit
		long timestamp = graph.getNow();
		// read the commit message
		Object metadata = graph.getCommitMetadata(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, timestamp);
		assertEquals(commitMessage, metadata);
	}

	@Test
	public void readingCommitMetadataFromNonExistingCommitTimestampsProducesNull() {
		ChronoGraph graph = this.getGraph();
		// the message we are going to store as metadata
		final String commitMessage = "Committed Hello World!";
		// add some data with our commit message
		graph.addVertex("message", "Hello World!");
		graph.tx().commit(commitMessage);
		// read commit metadata from a non-existing (but valid) commit timestamp
		assertNull(graph.getCommitMetadata(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, 0L));
	}

	@Test
	public void canUseHashMapAsCommitMetadata() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("message", "Hello World!");
		Map<String, String> commitMap = Maps.newHashMap();
		commitMap.put("User", "Martin");
		commitMap.put("Mood", "Good");
		commitMap.put("Mail", "martin.haeusler@uibk.ac.at");
		graph.tx().commit(commitMap);
		long timestamp = graph.getNow();

		// get the commit map
		@SuppressWarnings("unchecked")
		Map<String, String> retrieved = (Map<String, String>) graph
				.getCommitMetadata(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, timestamp);
		// assert that the maps are equal
		assertNotNull(retrieved);
		assertEquals(commitMap, retrieved);
	}
}
