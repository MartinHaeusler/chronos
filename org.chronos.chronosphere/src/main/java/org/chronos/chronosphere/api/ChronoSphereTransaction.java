package org.chronos.chronosphere.api;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronosphere.api.query.QueryStepBuilderStarter;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;

import com.google.common.collect.Iterators;

public interface ChronoSphereTransaction extends AutoCloseable {

	// =================================================================================================================
	// TRANSACTION METADATA
	// =================================================================================================================

	public long getTimestamp();

	public SphereBranch getBranch();

	// =================================================================================================================
	// ATTACHMENT & DELETION OPERATIONS
	// =================================================================================================================

	public EObject createAndAttach(EClass eClass);

	public void attach(EObject eObject);

	public void attach(Iterable<? extends EObject> eObjects);

	public void attach(Iterator<? extends EObject> eObjects);

	public default void delete(final EObject eObject) {
		this.delete(eObject, true);
	}

	public default void delete(final EObject eObject, final boolean cascadeDeletionToEContents) {
		delete(Iterators.singletonIterator(eObject), cascadeDeletionToEContents);
	}

	public default void delete(final Iterable<? extends EObject> eObjects) {
		this.delete(eObjects, true);
	}

	public default void delete(final Iterable<? extends EObject> eObjects, final boolean cascadeDeletionToEContents) {
		this.delete(eObjects.iterator(), cascadeDeletionToEContents);
	}

	public default void delete(final Iterator<? extends EObject> eObjects) {
		this.delete(eObjects, true);
	}

	public void delete(Iterator<? extends EObject> eObjects, boolean cascadeDeletionToEContents);

	// =================================================================================================================
	// EPACKAGE HANDLING
	// =================================================================================================================

	public EPackage getEPackageByNsURI(String namespaceURI);

	public Set<EPackage> getEPackages();

	public EPackage getEPackageByQualifiedName(String qualifiedName);

	public EClassifier getEClassifierByQualifiedName(String qualifiedName);

	public EClass getEClassByQualifiedName(String qualifiedName);

	public EStructuralFeature getFeatureByQualifiedName(String qualifiedName);

	public EAttribute getEAttributeByQualifiedName(String qualifiedName);

	public EReference getEReferenceByQualifiedName(String qualifiedName);

	public EPackage getEPackageBySimpleName(String simpleName);

	public EClassifier getEClassifierBySimpleName(String simpleName);

	public EClass getEClassBySimpleName(String simpleName);

	// =================================================================================================================
	// QUERY & RETRIEVAL OPERATIONS
	// =================================================================================================================

	/**
	 * Retrieves a single {@link ChronoEObject} by its unique identifier.
	 *
	 * @param eObjectID
	 *            The ID of the EObject to retrieve. Must not be <code>null</code>.
	 * @return The EObject. May be <code>null</code> if there is no EObject for the given ID.
	 */
	public ChronoEObject getEObjectById(String eObjectID);

	/**
	 * Retrieves the {@link ChronoEObject}s for the given unique identifiers.
	 *
	 * @param eObjectIDs
	 *            The IDs of the EObjects to retrieve. May be empty, but must not be <code>null</code>.
	 * @return The mapping from unique ID to retrieved EObject. May be empty, but never <code>null</code>. Is guaranteed
	 *         to include every given ID as a key. The IDs that did not have a matching EObject have <code>null</code>
	 *         values assigned in the map.
	 */
	public Map<String, ChronoEObject> getEObjectById(Iterable<String> eObjectIDs);

	/**
	 * Retrieves the {@link ChronoEObject}s for the given unique identifiers.
	 *
	 * @param eObjectIDs
	 *            The IDs of the EObjects to retrieve. May be empty, but must not be <code>null</code>.
	 * @return The mapping from unique ID to retrieved EObject. May be empty, but never <code>null</code>. Is guaranteed
	 *         to include every given ID as a key. The IDs that did not have a matching EObject have <code>null</code>
	 *         values assigned in the map.
	 */
	public Map<String, ChronoEObject> getEObjectById(Iterator<String> eObjectIDs);

	public QueryStepBuilderStarter find();

	// =================================================================================================================
	// HISTORY ANALYSIS
	// =================================================================================================================

	/**
	 * Returns an iterator over the change timestamps in the history of the given {@link EObject}.
	 *
	 * @param eObject
	 *            The eObject in question. Must not be <code>null</code>.
	 * @return An iterator over the change timestamps, in descending order. May be empty if the given object is not
	 *         attached.
	 */
	public Iterator<Long> getEObjectHistory(EObject eObject);

	/**
	 * Returns an iterator over all EObject modifications that have taken place in the given time range.
	 *
	 * @param timestampLowerBound
	 *            The lower bound of the time range to search in. Must not be negative. Must be less than or equal to
	 *            <code>timestampUpperBound</code>. Must be less than or equal to the transaction timestamp.
	 * @param timestampUpperBound
	 *            The upper bound of the time range to search in. Must not be negative. Must be greater than or equal to
	 *            <code>timestampLowerBound</code>. Must be less than or equal to the transaction timestamp.
	 *
	 * @return An iterator over pairs, containing the change timestamp at the first and the modified EObject id at the
	 *         second position. May be empty, but never <code>null</code>.
	 */
	public Iterator<Pair<Long, String>> getEObjectModificationsBetween(final long timestampLowerBound,
			final long timestampUpperBound);

	// =================================================================================================================
	// TRANSACTION CONTROL METHODS
	// =================================================================================================================

	public void commit();

	public void commit(Object commitMetadata);

	public void commitIncremental();

	public void rollback();

	public boolean isClosed();

	public boolean isOpen();

	@Override
	public void close(); // redefined from AutoClosable#close(), just without "throws Exception"

}
