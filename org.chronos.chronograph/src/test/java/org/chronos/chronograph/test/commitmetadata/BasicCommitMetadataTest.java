package org.chronos.chronograph.test.commitmetadata;

import static org.junit.Assert.*;

import java.util.Map;

import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;

@Category(IntegrationTest.class)
public class BasicCommitMetadataTest extends AllChronoGraphBackendsTest {

	@Test
	public void simpleStoringAndRetrievingMetadataWorks() {
		ChronoGraph graph = this.getGraph();
		// just a message that we can compare later on
		String commitMessage = "Hey there, Hello World!";
		// insert data into the database
		graph.addVertex("Hello", "World");
		graph.tx().commit(commitMessage);
		// record the commit timestamp
		long timestamp = graph.getNow();
		// read the metadata and assert that it's correct
		assertEquals(commitMessage, graph.getCommitMetadata(timestamp));
	}

	@Test
	public void readingCommitMetadataFromNonExistingCommitTimestampsProducesNull() {
		ChronoGraph graph = this.getGraph();
		// insert data into the database
		graph.addVertex("Hello", "World");
		graph.tx().commit("Hello World!");
		// read commit metadata from a non-existing (but valid) commit timestamp
		assertNull(graph.getCommitMetadata(0L));
	}

	@Test
	public void readingCommitMetadataRetrievesCorrectObject() {
		ChronoGraph graph = this.getGraph();
		// the commit messages, as constants to compare with later on
		final String commitMessage1 = "Hello World!";
		final String commitMessage2 = "Foo Bar";
		final String commitMessage3 = "Chronos is awesome";
		// insert data into the database
		graph.addVertex("Hello", "World");
		graph.tx().commit(commitMessage1);
		// record the commit timestamp
		long timeCommit1 = graph.getNow();

		this.sleep(5);

		// ... and another record
		graph.addVertex("Foo", "Bar");
		graph.tx().commit(commitMessage2);
		// record the commit timestamp
		long timeCommit2 = graph.getNow();

		this.sleep(5);

		// ... and yet another record
		graph.addVertex("Chronos", "IsAwesome");
		graph.tx().commit(commitMessage3);
		// record the commit timestamp
		long timeCommit3 = graph.getNow();

		this.sleep(5);

		// now, retrieve the three messages individually
		assertEquals(commitMessage1, graph.getCommitMetadata(timeCommit1));
		assertEquals(commitMessage2, graph.getCommitMetadata(timeCommit2));
		assertEquals(commitMessage3, graph.getCommitMetadata(timeCommit3));
	}

	@Test
	public void readingCommitMessagesFromOriginBranchWorks() {
		ChronoGraph graph = this.getGraph();
		// the commit messages, as constants to compare with later on
		final String commitMessage1 = "Hello World!";
		final String commitMessage2 = "Foo Bar";
		// insert data into the database
		graph.addVertex("Hello", "World");
		graph.tx().commit(commitMessage1);
		// record the commit timestamp
		long timeCommit1 = graph.getNow();

		this.sleep(5);

		// create the branch
		graph.getBranchManager().createBranch("MyBranch");

		// commit something to the branch
		ChronoGraph txGraph = graph.tx().createThreadedTx("MyBranch");
		try {
			txGraph.addVertex("Foo", "Bar");
			txGraph.tx().commit(commitMessage2);
		} finally {
			txGraph.close();
		}
		// record the commit timestamp
		long timeCommit2 = graph.getNow("MyBranch");

		// open a transaction on the branch and request the commit message of tx1
		assertEquals(commitMessage1, graph.getCommitMetadata("MyBranch", timeCommit1));
		// also, we should be able to get the commit metadata of the branch
		assertEquals(commitMessage2, graph.getCommitMetadata("MyBranch", timeCommit2));
	}

	@Test
	public void canUseHashMapAsCommitMetadata() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		Map<String, String> commitMap = Maps.newHashMap();
		commitMap.put("User", "Martin");
		commitMap.put("Mood", "Good");
		commitMap.put("Mail", "martin.haeusler@uibk.ac.at");
		graph.tx().commit(commitMap);
		long timestamp = graph.getNow();

		// get the commit map
		@SuppressWarnings("unchecked")
		Map<String, String> retrieved = (Map<String, String>) graph.getCommitMetadata(timestamp);
		// assert that the maps are equal
		assertNotNull(retrieved);
		assertEquals(commitMap, retrieved);
	}

}
