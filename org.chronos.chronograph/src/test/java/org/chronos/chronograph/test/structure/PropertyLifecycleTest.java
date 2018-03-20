package org.chronos.chronograph.test.structure;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.graph.PropertyStatus;
import org.chronos.chronograph.internal.impl.structure.graph.proxy.ChronoVertexProxy;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class PropertyLifecycleTest extends AllChronoGraphBackendsTest {

	@Test
	public void nonExistentPropertiesAreUnknown() {
		ChronoGraph g = this.getGraph();
		Vertex v = g.addVertex();
		assertThat(propertyStatus(v, "test"), is(PropertyStatus.UNKNOWN));
	}

	@Test
	public void newlyAddedPropertiesAreNew() {
		ChronoGraph g = this.getGraph();
		Vertex v = g.addVertex();
		v.property("hello", "world");
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.NEW));
	}

	@Test
	public void newPropertiesRemainNewWhenChanged() {
		ChronoGraph g = this.getGraph();
		Vertex v = g.addVertex();
		v.property("hello", "world");
		v.property("hello", "foo");
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.NEW));
	}

	@Test
	public void newPropertiesBecomeUnknownWhenRemoved() {
		ChronoGraph g = this.getGraph();
		Vertex v = g.addVertex();
		v.property("hello", "world");
		v.property("hello").remove();
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.UNKNOWN));
	}

	@Test
	public void newPropertiesBecomePersistedOnCommit() {
		ChronoGraph g = this.getGraph();
		Vertex v = g.addVertex();
		v.property("hello", "world");
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.NEW));
		g.tx().commit();
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.PERSISTED));
	}

	@Test
	public void persistedPropertiesBecomeModifiedOnChange() {
		ChronoGraph g = this.getGraph();
		Vertex v = g.addVertex();
		v.property("hello", "world");
		g.tx().commit();
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.PERSISTED));
		v.property("hello", "foo");
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.MODIFIED));
	}

	@Test
	public void canRemovePersistedProperty() {
		ChronoGraph g = this.getGraph();
		Vertex v = g.addVertex();
		v.property("hello", "world");
		g.tx().commit();
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.PERSISTED));
		v.property("hello").remove();
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.REMOVED));
	}

	@Test
	public void persistedPropertiesBecomeUnknownAfterRemoveAndCommit() {
		ChronoGraph g = this.getGraph();
		Vertex v = g.addVertex();
		v.property("hello", "world");
		g.tx().commit();
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.PERSISTED));
		v.property("hello").remove();
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.REMOVED));
		g.tx().commit();
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.UNKNOWN));
	}

	@Test
	public void modifiedPropertiesBecomePersistedOnCommit() {
		ChronoGraph g = this.getGraph();
		Vertex v = g.addVertex();
		v.property("hello", "world");
		g.tx().commit();
		v.property("hello", "foo");
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.MODIFIED));
		g.tx().commit();
		assertThat(propertyStatus(v, "hello"), is(PropertyStatus.PERSISTED));
	}

	// =================================================================================================================
	// HELPER METHODS
	// =================================================================================================================

	private static PropertyStatus propertyStatus(final Vertex vertex, final String key) {
		return vertexImpl(vertex).getPropertyStatus(key);
	}

	private static ChronoVertexImpl vertexImpl(final Vertex vertex) {
		ChronoVertex cv = (ChronoVertex) vertex;
		if (cv instanceof ChronoVertexProxy) {
			ChronoVertexProxy proxy = (ChronoVertexProxy) cv;
			return proxy.getElement();
		} else {
			return (ChronoVertexImpl) cv;
		}
	}
}
