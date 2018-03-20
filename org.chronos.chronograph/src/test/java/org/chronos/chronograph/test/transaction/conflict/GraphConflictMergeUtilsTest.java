package org.chronos.chronograph.test.transaction.conflict;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.exceptions.ChronoGraphCommitConflictException;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.conflict.PropertyConflictResolutionStrategy;
import org.chronos.chronograph.internal.impl.transaction.merge.GraphConflictMergeUtils;
import org.chronos.chronograph.test.base.ChronoGraphUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class GraphConflictMergeUtilsTest extends ChronoGraphUnitTest {

	// =================================================================================================================
	// ENVIRONMENT
	// We use an in-memory graph here because we don't want to test the actual graph itself, but only the static
	// helper methods in GraphConflictMergeUtils. To do so, we require TinkerPop-compliant instances of Vertex etc.,
	// and we only use the in-memory graph to provide those.
	// =================================================================================================================

	private ChronoGraph g;

	@Override
	@Before
	public void setup() {
		super.setup();
		this.g = ChronoGraph.FACTORY.create().inMemoryGraph().build();
	}

	@Override
	@After
	public void tearDown() {
		super.tearDown();
		this.g.close();
	}

	// =================================================================================================================
	// TESTS
	// =================================================================================================================

	@Test
	public void canAutoMergeNonConflictingCase() {
		Vertex vertex = this.g.addVertex();
		Vertex storeVertex = this.g.addVertex();
		vertex.property("hello", "world");
		storeVertex.property("hello", "world");
		PropertyConflictResolutionStrategy strategy = PropertyConflictResolutionStrategy.DO_NOT_MERGE;
		GraphConflictMergeUtils.mergeProperties(vertex, storeVertex, null, strategy);
		assertThat(vertex.property("hello").isPresent(), is(true));
		assertThat(vertex.value("hello"), is("world"));
	}

	@Test
	public void canAutoMergePropertyChangeInTransaction() {
		Vertex vertex = this.g.addVertex();
		Vertex storeVertex = this.g.addVertex();
		Vertex ancestorVertex = this.g.addVertex();
		vertex.property("hello", "foo");
		storeVertex.property("hello", "world");
		ancestorVertex.property("hello", "world");
		PropertyConflictResolutionStrategy strategy = PropertyConflictResolutionStrategy.DO_NOT_MERGE;
		GraphConflictMergeUtils.mergeProperties(vertex, storeVertex, ancestorVertex, strategy);
		assertThat(vertex.property("hello").isPresent(), is(true));
		assertThat(vertex.value("hello"), is("foo"));
	}

	@Test
	public void canAutoMergePropertyChangeInStore() {
		Vertex vertex = this.g.addVertex();
		Vertex storeVertex = this.g.addVertex();
		Vertex ancestorVertex = this.g.addVertex();
		vertex.property("hello", "world");
		storeVertex.property("hello", "foo");
		ancestorVertex.property("hello", "world");
		PropertyConflictResolutionStrategy strategy = PropertyConflictResolutionStrategy.DO_NOT_MERGE;
		GraphConflictMergeUtils.mergeProperties(vertex, storeVertex, ancestorVertex, strategy);
		assertThat(vertex.property("hello").isPresent(), is(true));
		assertThat(vertex.value("hello"), is("foo"));
	}

	@Test
	public void canAutoMergePropertyAddedInTransaction() {
		Vertex vertex = this.g.addVertex();
		Vertex storeVertex = this.g.addVertex();
		Vertex ancestorVertex = this.g.addVertex();
		vertex.property("hello", "world");
		PropertyConflictResolutionStrategy strategy = PropertyConflictResolutionStrategy.DO_NOT_MERGE;
		GraphConflictMergeUtils.mergeProperties(vertex, storeVertex, ancestorVertex, strategy);
		assertThat(vertex.property("hello").isPresent(), is(true));
		assertThat(vertex.value("hello"), is("world"));
	}

	@Test
	public void canAutoMergePropertyAddedInStore() {
		Vertex vertex = this.g.addVertex();
		Vertex storeVertex = this.g.addVertex();
		Vertex ancestorVertex = this.g.addVertex();
		storeVertex.property("hello", "world");
		PropertyConflictResolutionStrategy strategy = PropertyConflictResolutionStrategy.DO_NOT_MERGE;
		GraphConflictMergeUtils.mergeProperties(vertex, storeVertex, ancestorVertex, strategy);
		assertThat(vertex.property("hello").isPresent(), is(true));
		assertThat(vertex.value("hello"), is("world"));
	}

	@Test
	public void canAutoMergePropertyRemovedInTransaction() {
		Vertex vertex = this.g.addVertex();
		Vertex storeVertex = this.g.addVertex();
		Vertex ancestorVertex = this.g.addVertex();
		storeVertex.property("hello", "world");
		ancestorVertex.property("hello", "world");
		PropertyConflictResolutionStrategy strategy = PropertyConflictResolutionStrategy.DO_NOT_MERGE;
		GraphConflictMergeUtils.mergeProperties(vertex, storeVertex, ancestorVertex, strategy);
		assertThat(vertex.property("hello").isPresent(), is(false));
	}

	@Test
	public void canAutoMergePropertyRemovedInStore() {
		Vertex vertex = this.g.addVertex();
		Vertex storeVertex = this.g.addVertex();
		Vertex ancestorVertex = this.g.addVertex();
		vertex.property("hello", "world");
		ancestorVertex.property("hello", "world");
		PropertyConflictResolutionStrategy strategy = PropertyConflictResolutionStrategy.DO_NOT_MERGE;
		GraphConflictMergeUtils.mergeProperties(vertex, storeVertex, ancestorVertex, strategy);
		assertThat(vertex.property("hello").isPresent(), is(false));
	}

	@Test
	public void overwriteWithTransactionValueWorks() {
		Vertex vertex = this.g.addVertex();
		Vertex storeVertex = this.g.addVertex();
		Vertex ancestorVertex = this.g.addVertex();
		vertex.property("hello", "bar");
		storeVertex.property("hello", "foo");
		ancestorVertex.property("hello", "world");
		PropertyConflictResolutionStrategy strategy = PropertyConflictResolutionStrategy.OVERWRITE_WITH_TRANSACTION_VALUE;
		GraphConflictMergeUtils.mergeProperties(vertex, storeVertex, ancestorVertex, strategy);
		assertThat(vertex.property("hello").isPresent(), is(true));
		assertThat(vertex.value("hello"), is("bar"));
	}

	@Test
	public void overwriteWithStoreValueWorks() {
		Vertex vertex = this.g.addVertex();
		Vertex storeVertex = this.g.addVertex();
		Vertex ancestorVertex = this.g.addVertex();
		vertex.property("hello", "bar");
		storeVertex.property("hello", "foo");
		ancestorVertex.property("hello", "world");
		PropertyConflictResolutionStrategy strategy = PropertyConflictResolutionStrategy.OVERWRITE_WITH_STORE_VALUE;
		GraphConflictMergeUtils.mergeProperties(vertex, storeVertex, ancestorVertex, strategy);
		assertThat(vertex.property("hello").isPresent(), is(true));
		assertThat(vertex.value("hello"), is("foo"));
	}

	@Test
	public void doNotMergeWorks() {
		Vertex vertex = this.g.addVertex();
		Vertex storeVertex = this.g.addVertex();
		Vertex ancestorVertex = this.g.addVertex();
		vertex.property("hello", "bar");
		storeVertex.property("hello", "foo");
		ancestorVertex.property("hello", "world");
		PropertyConflictResolutionStrategy strategy = PropertyConflictResolutionStrategy.DO_NOT_MERGE;
		try {
			GraphConflictMergeUtils.mergeProperties(vertex, storeVertex, ancestorVertex, strategy);
			fail("Failed to trigger commit conflict exception!");
		} catch (ChronoGraphCommitConflictException expected) {
			// pass
		}
	}
}
