package org.chronos.chronograph.test.branch;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(IntegrationTest.class)
public class GraphBranchTest extends AllChronoGraphBackendsTest {

	@Test
	public void canRetrieveMasterBranch() {
		ChronoGraph graph = this.getGraph();
		GraphBranch masterBranch = graph.getBranchManager().getMasterBranch();
		checkNotNull(masterBranch, "Precondition violation - argument 'masterBranch' must not be NULL!");
		assertEquals(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, masterBranch.getName());
		assertEquals(0, masterBranch.getBranchingTimestamp());
		assertNull(masterBranch.getOrigin());
		assertTrue(masterBranch.getOriginsRecursive().isEmpty());
	}

	@Test
	public void canCreateBranch() {
		ChronoGraph graph = this.getGraph();
		// add some dummy test data
		Vertex vJohn = graph.addVertex("firstname", "John", "lastname", "Doe");
		graph.tx().commit();

		GraphBranch masterBranch = graph.getBranchManager().getMasterBranch();

		// create the branch
		GraphBranch branchScenarioA = graph.getBranchManager().createBranch("scenarioA");
		assertNotNull(branchScenarioA);
		assertEquals(masterBranch, branchScenarioA.getOrigin());
		assertEquals(Lists.newArrayList(masterBranch), branchScenarioA.getOriginsRecursive());

		// add test data to the branch
		graph.tx().open(branchScenarioA.getName());
		Vertex vJane = graph.addVertex("firstname", "Jane", "lastname", "Doe");
		vJohn.addEdge("married", vJane);
		vJane.addEdge("married", vJohn);

		graph.tx().commit();

		// in the master branch, we shouldn't see the change
		graph.tx().open();
		assertFalse(vJohn.edges(Direction.OUT, "married").hasNext());
		graph.tx().close();

		// in the scenario branch, we should see the changes
		graph.tx().open(branchScenarioA.getName());
		assertEquals(vJane, vJohn.edges(Direction.OUT, "married").next().inVertex());
		graph.tx().close();
	}

}
