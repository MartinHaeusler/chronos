package org.chronos.chronograph.internal.api.index;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;

import org.chronos.chronodb.internal.api.query.SearchSpecification;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.structure.ChronoGraph;

public interface ChronoGraphIndexManagerInternal extends ChronoGraphIndexManager {

	// =====================================================================================================================
	// INDEX MANIPULATION
	// =====================================================================================================================

	public void addIndex(ChronoGraphIndex index);

	// =====================================================================================================================
	// INDEX QUERYING
	// =====================================================================================================================

	public Iterator<String> findVertexIdsByIndexedProperties(final Set<SearchSpecification> searchSpecifications);

	public Iterator<String> findEdgeIdsByIndexedProperties(final Set<SearchSpecification> searchSpecifications);

	// =====================================================================================================================
	// UTILITY
	// =====================================================================================================================

	public void executeOnGraph(ChronoGraph graph, Runnable job);

	public <T> T executeOnGraph(ChronoGraph graph, Callable<T> job);

}
