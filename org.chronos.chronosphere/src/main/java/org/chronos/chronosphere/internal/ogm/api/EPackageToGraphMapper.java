package org.chronos.chronosphere.internal.ogm.api;

import org.chronos.chronograph.api.structure.ChronoGraph;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

/**
 * This class is a utility capable of mapping {@link EPackage}s to the graph, and reading the
 * {@link ChronoEPackageRegistry} from the graph.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface EPackageToGraphMapper {

	/**
	 * Maps the contents of the given {@link EPackageBundle} into the given {@link ChronoGraph}.
	 *
	 * <p>
	 * After this operation, the contents of the graph will fully reflect the contents of the given
	 * {@link EPackageBundle}. The graph will be adapted, the bundle is treated as read-only.
	 *
	 * <p>
	 * <b>/!\ WARNING:</b> If an EClass or (sub-) package was removed, the instances will be deleted as well!
	 *
	 * @param graph
	 *            The graph to map the EPackageBundle into. Must not be <code>null</code>. Must be self-contained.
	 * @param bundle
	 *            The bundle to synchronize with the graph. Will be treated as read-only. Must not be <code>null</code>.
	 */
	void mapToGraph(ChronoGraph graph, EPackageBundle bundle);

	/**
	 * Reads the contents of the given {@link ChronoEPackageRegistry} from the graph.
	 *
	 * @param graph
	 *            The graph to read from. Must not be <code>null</code>.
	 * @return The {@link ChronoEPackageRegistry}. Never <code>null</code>.
	 */
	public ChronoEPackageRegistry readChronoEPackageRegistryFromGraph(ChronoGraph graph);

	/**
	 * Removes the graph representation of the given {@link EPackageBundle}, as well as all {@link EObject}s that are
	 * instances of the classifiers contained within the bundle.
	 *
	 * <p>
	 * This is an expensive operation. Use with care.
	 *
	 * @param graph
	 *            The graph to perform the deletion in. Must not be <code>null</code>.
	 * @param bundle
	 *            The {@link EPackageBundle} to delete in the graph. Must not be <code>null</code>.
	 */
	public void deleteInGraph(ChronoGraph graph, EPackageBundle bundle);

}