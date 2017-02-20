package org.chronos.chronograph.test.id;

import static org.junit.Assert.*;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class IdsUniqueTest extends AllChronoGraphBackendsTest {

	@Test
	public void shouldHaveExceptionConsistencyWhenAssigningSameIdOnEdge() {
		final Vertex v = this.getGraph().addVertex();
		final Object o = "1";
		v.addEdge("self", v, T.id, o);
		try {
			v.addEdge("self", v, T.id, o);
			fail("Assigning the same ID to an Element should throw an exception");
		} catch (IllegalArgumentException expected) {
			// pass
		}
	}

}
