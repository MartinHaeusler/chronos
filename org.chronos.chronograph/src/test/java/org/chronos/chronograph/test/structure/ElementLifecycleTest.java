package org.chronos.chronograph.test.structure;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.NoSuchElementException;

import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.internal.impl.structure.graph.ElementLifecycleStatus;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterators;

@Category(IntegrationTest.class)
public class ElementLifecycleTest extends AllChronoGraphBackendsTest {

	@Test
	public void stateNewIsPreservedDuringEditing() {
		ChronoGraph g = this.getGraph();
		ChronoVertex v1 = (ChronoVertex) g.addVertex();
		// the state should be NEW after creating a vertex
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.NEW));
		// if we change a property, the state should still be NEW
		v1.property("hello", "world");
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.NEW));
		ChronoVertex v2 = (ChronoVertex) g.addVertex();
		// if we add an edge to the vertex, the status should still be NEW
		ChronoEdge e1 = (ChronoEdge) v1.addEdge("test", v2);
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.NEW));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.NEW));
		// if we set a property on the edge, it's state should still be NEW
		e1.property("hello", "world");
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.NEW));
		// if we remove the edge, the vertices should still be NEW
		e1.remove();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.NEW));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.NEW));
	}

	@Test
	public void removingNewVertexMakesItObsolete() {
		ChronoGraph g = this.getGraph();
		ChronoVertex v1 = (ChronoVertex) g.addVertex();
		// the state should be NEW after creating a vertex
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.NEW));
		v1.remove();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.OBSOLETE));
	}

	@Test
	public void removingNewEdgeMakesItObsolete() {
		ChronoGraph g = this.getGraph();
		ChronoVertex v1 = (ChronoVertex) g.addVertex();
		ChronoVertex v2 = (ChronoVertex) g.addVertex();
		ChronoEdge e1 = (ChronoEdge) v1.addEdge("test", v2);
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.NEW));
		e1.remove();
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.OBSOLETE));
	}

	@Test
	public void savingAnElementSwitchesStateToPersistent() {
		ChronoGraph g = this.getGraph();
		ChronoVertex v1 = (ChronoVertex) g.addVertex();
		ChronoVertex v2 = (ChronoVertex) g.addVertex();
		ChronoEdge e1 = (ChronoEdge) v1.addEdge("test", v2);
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.NEW));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.NEW));
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.NEW));
		g.tx().commit();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
	}

	@Test
	public void loadingAnElementFromTheStoreSwitchesStateToPersistent() {
		ChronoGraph g = this.getGraph();
		ChronoVertex v1 = (ChronoVertex) g.addVertex();
		ChronoVertex v2 = (ChronoVertex) g.addVertex();
		ChronoEdge e1 = (ChronoEdge) v1.addEdge("test", v2);
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.NEW));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.NEW));
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.NEW));
		g.tx().commit();
		ChronoVertex v1Loaded = (ChronoVertex) Iterators.getOnlyElement(g.vertices(v1.id()));
		ChronoVertex v2Loaded = (ChronoVertex) Iterators.getOnlyElement(g.vertices(v2.id()));
		ChronoEdge e1Loaded = (ChronoEdge) Iterators.getOnlyElement(g.edges(e1.id()));
		assertThat(v1Loaded.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(v2Loaded.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(e1Loaded.getStatus(), is(ElementLifecycleStatus.PERSISTED));
	}

	@Test
	public void addingOrRemovingAnEdgeToPersistentVertexSwitchesStateToEdgeChange() {
		ChronoGraph g = this.getGraph();
		ChronoVertex v1 = (ChronoVertex) g.addVertex();
		ChronoVertex v2 = (ChronoVertex) g.addVertex();
		g.tx().commit();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		// adding an edge should switch the vertex state to EDGE_CHANGED
		ChronoEdge e1 = (ChronoEdge) v1.addEdge("test", v2);
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.NEW));
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.EDGE_CHANGED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.EDGE_CHANGED));
		g.tx().commit();
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		// remove the edge, switching the adjacent vertices to EDGE_CHANGED
		e1.remove();
		assertRemoved(e1);
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.EDGE_CHANGED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.EDGE_CHANGED));
		g.tx().commit();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertRemoved(e1);
	}

	@Test
	public void changingAPropertyOnAPersistentVertexSwitchesStateToPropertyChanged() {
		ChronoGraph g = this.getGraph();
		ChronoVertex v1 = (ChronoVertex) g.addVertex();
		ChronoVertex v2 = (ChronoVertex) g.addVertex();
		ChronoEdge e1 = (ChronoEdge) v1.addEdge("test", v2);
		g.tx().commit();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		v1.property("hello", "world");
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PROPERTY_CHANGED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		g.tx().commit();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
	}

	@Test
	public void changingAPropertyOnAVertexInEdgeChangeStateSwitchesStateToPropertyChange() {
		ChronoGraph g = this.getGraph();
		ChronoVertex v1 = (ChronoVertex) g.addVertex();
		ChronoVertex v2 = (ChronoVertex) g.addVertex();
		g.tx().commit();
		ChronoEdge e1 = (ChronoEdge) v1.addEdge("test", v2);
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.EDGE_CHANGED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.EDGE_CHANGED));
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.NEW));
		v1.property("hello", "world");
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PROPERTY_CHANGED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.EDGE_CHANGED));
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.NEW));
		g.tx().commit();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
	}

	@Test
	public void vertexInStatePropertyChangedCannotSwitchBackToEdgeChanged() {
		ChronoGraph g = this.getGraph();
		ChronoVertex v1 = (ChronoVertex) g.addVertex();
		ChronoVertex v2 = (ChronoVertex) g.addVertex();
		ChronoEdge e1 = (ChronoEdge) v1.addEdge("test", v2);
		g.tx().commit();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		v1.property("hello", "world");
		e1.remove();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PROPERTY_CHANGED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.EDGE_CHANGED));
		assertRemoved(e1);
		g.tx().commit();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertRemoved(e1);
	}

	@Test
	public void removingNeighborVertexSwitchesPersistentVertexToStateEdgeChanged() {
		ChronoGraph g = this.getGraph();
		ChronoVertex v1 = (ChronoVertex) g.addVertex();
		ChronoVertex v2 = (ChronoVertex) g.addVertex();
		ChronoVertex v3 = (ChronoVertex) g.addVertex();
		ChronoEdge e1 = (ChronoEdge) v1.addEdge("test", v2);
		ChronoEdge e2 = (ChronoEdge) v2.addEdge("test", v3);
		g.tx().commit();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(v2.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(v3.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(e1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertThat(e2.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		v2.remove();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.EDGE_CHANGED));
		assertRemoved(v2);
		assertThat(v3.getStatus(), is(ElementLifecycleStatus.EDGE_CHANGED));
		assertRemoved(e1);
		assertRemoved(e2);
		g.tx().commit();
		assertThat(v1.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertRemoved(v2);
		assertThat(v3.getStatus(), is(ElementLifecycleStatus.PERSISTED));
		assertRemoved(e1);
		assertRemoved(e2);
	}

	private static void assertRemoved(final ChronoElement element) {
		try {
			assertThat(element.getStatus(), is(ElementLifecycleStatus.REMOVED));
		} catch (NoSuchElementException expected) {
			// pass
		}
	}
}
