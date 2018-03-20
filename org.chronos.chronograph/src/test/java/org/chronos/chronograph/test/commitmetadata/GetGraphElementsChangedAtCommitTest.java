package org.chronos.chronograph.test.commitmetadata;

import static org.junit.Assert.*;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class GetGraphElementsChangedAtCommitTest extends AllChronoGraphBackendsTest {

	@Test
	public void canRetrieveChangesFromMasterBranch() {
		ChronoGraph g = this.getGraph();
		Vertex vJohn = g.addVertex("name", "John");
		String idJohn = (String) vJohn.id();
		Vertex vJane = g.addVertex("name", "Jane");
		String idJane = (String) vJane.id();
		vJohn.addEdge("married", vJane);
		g.tx().commit();
		long commit1 = g.getNow();
		Vertex vSarah = g.addVertex("name", "Sarah");
		String idSarah = (String) vSarah.id();
		vJohn.addEdge("father", vSarah);
		vJane.addEdge("mother", vSarah);
		g.tx().commit();
		long commit2 = g.getNow();
		// removing john will also delete the edges, which will change jane and sarah
		vJohn.remove();
		g.tx().commit();
		long commit3 = g.getNow();
		vJane.property("test", "value");
		g.tx().commit();
		long commit4 = g.getNow();
		assertEquals(Sets.newHashSet(idJohn, idJane), Sets.newHashSet(g.getChangedVerticesAtCommit(commit1)));
		assertEquals(Sets.newHashSet(idJohn, idJane, idSarah), Sets.newHashSet(g.getChangedVerticesAtCommit(commit2)));
		assertEquals(Sets.newHashSet(idJohn, idJane, idSarah), Sets.newHashSet(g.getChangedVerticesAtCommit(commit3)));
		assertEquals(Sets.newHashSet(idJane), Sets.newHashSet(g.getChangedVerticesAtCommit(commit4)));
	}

	@Test
	public void canRetrieveChangesFromOtherBranch() {
		ChronoGraph g = this.getGraph();
		Vertex vJohn = g.addVertex("name", "John");
		String idJohn = (String) vJohn.id();
		Vertex vJane = g.addVertex("name", "Jane");
		String idJane = (String) vJane.id();
		vJohn.addEdge("married", vJane);
		g.tx().commit();
		long commit1 = g.getNow();
		g.getBranchManager().createBranch("test");

		g.tx().open("test");
		Vertex vSarah = g.addVertex("name", "Sarah");
		String idSarah = (String) vSarah.id();
		vJohn.addEdge("father", vSarah);
		vJane.addEdge("mother", vSarah);
		g.tx().commit();
		long commit2 = g.getNow("test");

		g.tx().open("test");
		// removing john will also delete the edges, which will change jane and sarah
		vJohn.remove();
		g.tx().commit();
		long commit3 = g.getNow("test");

		g.tx().open("test");
		vJane.property("test", "value");
		g.tx().commit();
		long commit4 = g.getNow("test");

		assertEquals(Sets.newHashSet(idJohn, idJane), Sets.newHashSet(g.getChangedVerticesAtCommit("test", commit1)));
		assertEquals(Sets.newHashSet(idJohn, idJane, idSarah),
				Sets.newHashSet(g.getChangedVerticesAtCommit("test", commit2)));
		assertEquals(Sets.newHashSet(idJohn, idJane, idSarah),
				Sets.newHashSet(g.getChangedVerticesAtCommit("test", commit3)));
		assertEquals(Sets.newHashSet(idJane), Sets.newHashSet(g.getChangedVerticesAtCommit("test", commit4)));
	}

}
