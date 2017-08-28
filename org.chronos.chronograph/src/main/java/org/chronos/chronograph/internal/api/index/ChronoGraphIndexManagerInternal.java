package org.chronos.chronograph.internal.api.index;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;

import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.structure.ChronoGraph;

/**
 * The internal representation of the {@link ChronoGraphIndexManager} with additional methods for internal use.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoGraphIndexManagerInternal extends ChronoGraphIndexManager {

	// =====================================================================================================================
	// INDEX MANIPULATION
	// =====================================================================================================================

	/**
	 * Adds the given graph index to this manager.
	 *
	 * @param index
	 *            The index to add. Must not be <code>null</code>.
	 */
	public void addIndex(ChronoGraphIndex index);

	// =====================================================================================================================
	// INDEX QUERYING
	// =====================================================================================================================

	/**
	 * Performs an index search for the vertices that meet <b>all</b> of the given search specifications.
	 *
	 * @param searchSpecifications
	 *            The search specifications to find the matching vertices for. Must not be <code>null</code>.
	 *
	 * @return An iterator over the IDs of all vertices that fulfill all given search specifications. May be empty, but never <code>null</code>.
	 */
	public Iterator<String> findVertexIdsByIndexedProperties(final Set<SearchSpecification<?>> searchSpecifications);

	/**
	 * Performs an index search for the edges that meet <b>all</b> of the given search specifications.
	 *
	 * @param searchSpecifications
	 *            The search specifications to find the matching edges for. Must not be <code>null</code>.
	 *
	 * @return An iterator over the IDs of all edges that fulfill all given search specifications. May be empty, but never <code>null</code>.
	 */
	public Iterator<String> findEdgeIdsByIndexedProperties(final Set<SearchSpecification<?>> searchSpecifications);

	// =====================================================================================================================
	// UTILITY
	// =====================================================================================================================

	/**
	 * Instances of {@link ChronoGraphIndexManager} internally hold a reference to their owning {@link ChronoGraph}. This method allows to temporarily exchange that reference with the given graph and execute the given job.
	 *
	 * @param graph
	 *            The graph to refer to during the execution of the given job. Must not be <code>null</code>.
	 * @param job
	 *            The job to execute on this manager while it is referring to the given graph. Must not be <code>null</code>.
	 */
	public void executeOnGraph(ChronoGraph graph, Runnable job);

	/**
	 * Instances of {@link ChronoGraphIndexManager} internally hold a reference to their owning {@link ChronoGraph}. This method allows to temporarily exchange that reference with the given graph and execute the given job.
	 *
	 * @param graph
	 *            The graph to refer to during the execution of the given job. Must not be <code>null</code>.
	 * @param job
	 *            The job to execute on this manager while it is referring to the given graph. Must not be <code>null</code>.
	 * 
	 * @return The result of the job.
	 */
	public <T> T executeOnGraph(ChronoGraph graph, Callable<T> job);

}
