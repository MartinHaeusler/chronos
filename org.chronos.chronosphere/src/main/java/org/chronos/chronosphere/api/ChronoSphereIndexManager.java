package org.chronos.chronosphere.api;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EPackage;

/**
 * This class manages the individual secondary indices on a model.
 *
 * <p>
 * You can get an instance of this class via {@link ChronoSphere#getIndexManager()}.
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoSphereIndexManager {

	/**
	 * Creates an index on the given {@link EAttribute}.
	 *
	 * @param eAttribute
	 *            The attribute to index. Must not be <code>null</code>. Must be part of a registered {@link EPackage}.
	 *
	 * @return <code>true</code> if a new index for the given EAttribute was created, or <code>false</code> if the index already existed.
	 *
	 * @throws IllegalArgumentException
	 *             Thrown if the given EAttribute is not part of any registered EPackage.
	 * @throws NullPointerException
	 *             Thrown if the given EAttribute is <code>null</code>.
	 *
	 * @see ChronoSphere#getEPackageManager()
	 * @see ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage)
	 */
	public boolean createIndexOn(EAttribute eAttribute);

	/**
	 * Checks if the given {@link EAttribute} is indexed or not.
	 *
	 * @param eAttribute
	 *            The EAttribute to check. Must not be <code>null</code>. Must be part of a registered {@link EPackage}.
	 *
	 * @return <code>true</code> if the attribute is indexed, otherwise <code>false</code>.
	 *
	 * @throws IllegalArgumentException
	 *             Thrown if the given EAttribute is not part of any registered EPackage.
	 * @throws NullPointerException
	 *             Thrown if the given EAttribute is <code>null</code>.
	 *
	 * @see ChronoSphere#getEPackageManager()
	 * @see ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage)
	 */
	public boolean existsIndexOn(EAttribute eAttribute);

	/**
	 * Drops an existing index on the given {@link EAttribute} (if such an index exists).
	 *
	 * @param eAttribute
	 *            The EAttribute to drop the index for. Must not be <code>null</code>. Must be part of a registered {@link EPackage}.
	 *
	 * @return <code>true</code> if an index on the given EAttribute existed and was dropped successfully, or <code>false</code> if there was no such index.
	 *
	 * @throws IllegalArgumentException
	 *             Thrown if the given EAttribute is not part of any registered EPackage.
	 * @throws NullPointerException
	 *             Thrown if the given EAttribute is <code>null</code>.
	 *
	 * @see ChronoSphere#getEPackageManager()
	 * @see ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage)
	 */
	public boolean dropIndexOn(EAttribute eAttribute);

	/**
	 * Re-indexes all dirty indices.
	 *
	 * <p>
	 * This is a potentially expensive operation that can take a long time, depending on the size of the model stored in the repository. Use it with care.
	 */
	public void reindexAll();

	/**
	 * Rebuilds the index on the given {@link EAttribute} from scratch.
	 *
	 * <p>
	 * This is a potentially expensive operation that can take a long time, depending on the size of the model stored in the repository. Use it with care.
	 *
	 * @param eAttribute
	 *            The EAttribute to rebuild the index for. Must not be <code>null</code>. Must be part of a registered {@link EPackage}. If there is no index on the given EAttribute, this method is a no-op and returns immediately.
	 *
	 * @throws IllegalArgumentException
	 *             Thrown if the given EAttribute is not part of any registered EPackage.
	 * @throws NullPointerException
	 *             Thrown if the given EAttribute is <code>null</code>.
	 *
	 * @see #isIndexDirty(EAttribute)
	 * @see ChronoSphere#getEPackageManager()
	 * @see ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage)
	 *
	 * @deprecated As of Chronos 0.6.8 or later, please use {@link #reindexAll()} instead.
	 *
	 */
	@Deprecated
	public void reindex(EAttribute eAttribute);

	/**
	 * Checks if the index on the given {@link EAttribute} is dirty.
	 *
	 * <p>
	 * An index can become dirty e.g. when a new index is created on an already existing model, or when the {@link EPackage} contents change. A dirty index is an index that is out-of-synch with the model data and needs to be re-synchronized. This method checks if such re-synchronization is required.
	 *
	 * @param eAttribute
	 *            The EAttribute to check the dirty state of its index for. Must not be <code>null</code>. Must be part of a registered {@link EPackage}. If there is no index on the given EAttribute, this method returns <code>false</code>.
	 *
	 * @return <code>true</code> if there is an index on the given EAttribute AND that index is dirty, <code>false</code> either if there is no index on the given EAttribute or that index is not dirty.
	 *
	 * @throws IllegalArgumentException
	 *             Thrown if the given EAttribute is not part of any registered EPackage.
	 * @throws NullPointerException
	 *             Thrown if the given EAttribute is <code>null</code>.
	 *
	 * @see #isIndexDirty(EAttribute)
	 * @see ChronoSphere#getEPackageManager()
	 * @see ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage)
	 */
	public boolean isIndexDirty(EAttribute eAttribute);

}
