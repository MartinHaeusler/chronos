package org.chronos.chronograph.test.builder;

import static org.junit.Assert.*;

import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ChronoGraphCreationTest extends AllChronoGraphBackendsTest {

	@Test
	public void graphCreationWorks() {
		ChronoGraph graph = this.getGraph();
		assertNotNull("Failed to instantiate ChronoGraph on Backend '" + this.getChronoBackendName() + "'!", graph);
	}
}
