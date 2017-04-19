package org.chronos.chronosphere.impl.transaction;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.internal.util.IteratorUtils;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronosphere.api.SphereBranch;
import org.chronos.chronosphere.api.query.QueryStepBuilderStarter;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.emf.impl.ChronoEFactory;
import org.chronos.chronosphere.emf.impl.ChronoEObjectImpl;
import org.chronos.chronosphere.emf.internal.api.ChronoEObjectInternal;
import org.chronos.chronosphere.emf.internal.impl.store.ChronoGraphEStore;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.impl.query.QueryStepBuilderStarterImpl;
import org.chronos.chronosphere.internal.api.ChronoSphereInternal;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.chronos.chronosphere.internal.ogm.api.VertexKind;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.util.EcoreUtil;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ChronoSphereTransactionImpl implements ChronoSphereTransactionInternal {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private ChronoEPackageRegistry ePackageRegistry;

	private final ChronoSphereInternal owningSphere;
	private final ChronoGraph txGraph;
	private final ChronoGraphTransaction tx;
	private final ChronoGraphEStore graphEStore;
	private boolean closed;

	// =================================================================================================================
	// CONSTRUCTORS
	// =================================================================================================================

	public ChronoSphereTransactionImpl(final ChronoSphereInternal owningSphere, final ChronoGraph txGraph) {
		checkNotNull(owningSphere, "Precondition violation - argument 'owningSphere' must not be NULL!");
		checkNotNull(txGraph, "Precondition violation - argument 'txGraph' must not be NULL!");
		this.owningSphere = owningSphere;
		this.txGraph = txGraph;
		this.tx = txGraph.tx().getCurrentTransaction();
		this.ePackageRegistry = this.owningSphere.getEPackageToGraphMapper()
				.readChronoEPackageRegistryFromGraph(txGraph);
		this.graphEStore = new ChronoGraphEStore(this);
	}

	// =================================================================================================================
	// TRANSACTION METADATA
	// =================================================================================================================

	@Override
	public long getTimestamp() {
		return this.tx.getTimestamp();
	}

	@Override
	public SphereBranch getBranch() {
		return this.owningSphere.getBranchManager().getBranch(this.tx.getBranchName());
	}

	// =================================================================================================================
	// ATTACHMENT & DELETION OPERATIONS
	// =================================================================================================================

	@Override
	public EObject createAndAttach(final EClass eClass) {
		checkArgument(this.getEPackageRegistry().getEClassID(eClass) != null,
				"Precondition violation - the given EClass is not known in the repository!");
		checkArgument(eClass.getEPackage().getEFactoryInstance() instanceof ChronoEFactory,
				"Precondition violation - the given EClass is not known in the repository!");
		EObject eObject = EcoreUtil.create(eClass);
		this.attach(eObject);
		return eObject;
	}

	@Override
	public EObject createAndAttach(final EClass eClass, final String eObjectID) {
		checkArgument(this.getEPackageRegistry().getEClassID(eClass) != null,
				"Precondition violation - the given EClass is not known in the repository!");
		checkArgument(eClass.getEPackage().getEFactoryInstance() instanceof ChronoEFactory,
				"Precondition violation - the given EClass is not known in the repository!");
		checkNotNull(eObjectID, "Precondition violation - argument 'eObjectID' must not be NULL!");
		EObject eObject = new ChronoEObjectImpl(eObjectID, eClass);
		this.attach(eObject);
		return eObject;
	}

	@Override
	public void attach(final EObject eObject) {
		ChronoEObjectInternal eObjectInternal = this.assertIsChronoEObject(eObject);
		if (eObjectInternal.isAttached()) {
			// already attached to the store, nothing to do
			return;
		}
		this.attach(Collections.singleton(eObject));
	}

	@Override
	public void attach(final Iterable<? extends EObject> eObjects) {
		checkNotNull(eObjects, "Precondition violation - argument 'eObjects' must not be NULL!");
		this.attach(eObjects.iterator());
	}

	@Override
	public void attach(final Iterator<? extends EObject> eObjects) {
		checkNotNull(eObjects, "Precondition violation - argument 'eObjects' must not be NULL!");
		this.attach(eObjects, false);
	}

	@Override
	public void delete(final EObject eObject) {
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		this.delete(Collections.singleton(eObject));
	}

	@Override
	public void delete(final Iterator<? extends EObject> eObjects, final boolean cascadeDeletionToEContents) {
		checkNotNull(eObjects, "Precondition violation - argument 'eObjects' must not be NULL!");
		if (eObjects.hasNext() == false) {
			return;
		}
		// consider only attached EObjects
		Set<ChronoEObjectInternal> eObjectsToDelete = IteratorUtils.stream(eObjects)
				// only consider our internal EObjects (anything else can't be added anyways)
				.filter(eObject -> eObject instanceof ChronoEObjectInternal)
				.map(eObject -> (ChronoEObjectInternal) eObject)
				// consider only the ones that are currently attached
				.filter(eObject -> eObject.isAttached())
				// collect them in a set
				.collect(Collectors.toSet());
		if (eObjectsToDelete.isEmpty()) {
			// there's nothing to delete
			return;
		}
		this.getGraphEStore().deepDelete(eObjectsToDelete, cascadeDeletionToEContents);
	}

	// =================================================================================================================
	// EPACKAGE HANDLING
	// =================================================================================================================

	@Override
	public EPackage getEPackageByNsURI(final String namespaceURI) {
		checkNotNull(namespaceURI, "Precondition violation - argument 'namespaceURI' must not be NULL!");
		this.assertNotClosed();
		return this.getEPackageRegistry().getEPackage(namespaceURI);
	}

	@Override
	public Set<EPackage> getEPackages() {
		this.assertNotClosed();
		return Collections.unmodifiableSet(this.getEPackageRegistry().getEPackages());
	}

	@Override
	public EPackage getEPackageByQualifiedName(final String qualifiedName) {
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		return EMFUtils.getEPackageByQualifiedName(this.getEPackages(), qualifiedName);
	}

	@Override
	public EClassifier getEClassifierByQualifiedName(final String qualifiedName) {
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		return EMFUtils.getEClassifierByQualifiedName(this.getEPackages(), qualifiedName);
	}

	@Override
	public EClass getEClassByQualifiedName(final String qualifiedName) {
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		return EMFUtils.getEClassByQualifiedName(this.getEPackages(), qualifiedName);
	}

	@Override
	public EStructuralFeature getFeatureByQualifiedName(final String qualifiedName) {
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		return EMFUtils.getFeatureByQualifiedName(this.getEPackages(), qualifiedName);
	}

	@Override
	public EAttribute getEAttributeByQualifiedName(final String qualifiedName) {
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		return EMFUtils.getEAttributeByQualifiedName(this.getEPackages(), qualifiedName);
	}

	@Override
	public EReference getEReferenceByQualifiedName(final String qualifiedName) {
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		return EMFUtils.getEReferenceByQualifiedName(this.getEPackages(), qualifiedName);
	}

	@Override
	public EPackage getEPackageBySimpleName(final String simpleName) {
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		return EMFUtils.getEPackageBySimpleName(this.getEPackages(), simpleName);
	}

	@Override
	public EClassifier getEClassifierBySimpleName(final String simpleName) {
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		return EMFUtils.getEClassifierBySimpleName(this.getEPackages(), simpleName);
	}

	@Override
	public EClass getEClassBySimpleName(final String simpleName) {
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		return EMFUtils.getEClassBySimpleName(this.getEPackages(), simpleName);
	}

	// =================================================================================================================
	// QUERY & RETRIEVAL OPERATIONS
	// =================================================================================================================

	@Override
	public ChronoEObject getEObjectById(final String eObjectID) {
		checkNotNull(eObjectID, "Precondition violation - argument 'eObjectID' must not be NULL!");
		Map<String, ChronoEObject> map = this.getEObjectById(Iterators.singletonIterator(eObjectID));
		if (map == null || map.isEmpty()) {
			return null;
		}
		ChronoEObject eObject = map.get(eObjectID);
		return eObject;
	}

	@Override
	public Map<String, ChronoEObject> getEObjectById(final Iterable<String> eObjectIDs) {
		checkNotNull(eObjectIDs, "Precondition violation - argument 'eObjectIDs' must not be NULL!");
		return this.getEObjectById(eObjectIDs.iterator());
	}

	@Override
	public Map<String, ChronoEObject> getEObjectById(final Iterator<String> eObjectIDs) {
		checkNotNull(eObjectIDs, "Precondition violation - argument 'eObjectIDs' must not be NULL!");
		Map<String, ChronoEObject> resultMap = Maps.newHashMap();
		eObjectIDs.forEachRemaining(id -> {
			Vertex vertex = ChronoSphereGraphFormat.getVertexForEObject(this.getGraph(), id);
			if (vertex == null) {
				resultMap.put(id, null);
			} else {
				ChronoEObject eObject = this.createEObjectFromVertex(vertex);
				resultMap.put(id, eObject);
			}
		});
		return resultMap;
	}

	@Override
	public QueryStepBuilderStarter find() {
		return new QueryStepBuilderStarterImpl(this);
	}

	// =================================================================================================================
	// HISTORY ANALYSIS
	// =================================================================================================================

	@Override
	public Iterator<Long> getEObjectHistory(final EObject eObject) {
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		if (eObject instanceof ChronoEObjectInternal == false) {
			return Collections.emptyIterator();
		}
		ChronoEObjectInternal eObjectInternal = (ChronoEObjectInternal) eObject;
		String id = eObjectInternal.getId();
		return this.txGraph.getVertexHistory(id);
	}

	@Override
	public Iterator<Pair<Long, String>> getEObjectModificationsBetween(final long timestampLowerBound,
			final long timestampUpperBound) {
		checkArgument(timestampLowerBound >= 0,
				"Precondition violation - argument 'timestampLowerBound' must not be negative!");
		checkArgument(timestampUpperBound >= 0,
				"Precondition violation - argument 'timestampUpperBound' must not be negative!");
		checkArgument(timestampLowerBound < timestampUpperBound,
				"Precondition violation - argument 'timestampLowerBound' must be strictly smaller than argument 'timestampUpperBound'!");
		return this.txGraph.getVertexModificationsBetween(timestampLowerBound, timestampUpperBound);
	}

	// =================================================================================================================
	// TRANSACTION CONTROL METHODS
	// =================================================================================================================

	@Override
	public void commit() {
		this.assertNotClosed();
		this.tx.commit();
		this.closed = true;
	}

	@Override
	public void commit(final Object commitMetadata) {
		this.assertNotClosed();
		this.tx.commit(commitMetadata);
		this.closed = true;
	}

	@Override
	public void commitIncremental() {
		this.assertNotClosed();
		this.tx.commitIncremental();
	}

	@Override
	public void rollback() {
		this.assertNotClosed();
		this.tx.rollback();
		this.closed = true;
	}

	@Override
	public boolean isClosed() {
		return this.closed || this.tx.isOpen() == false;
	}

	@Override
	public boolean isOpen() {
		return this.isClosed() == false;
	}

	@Override
	public void close() {
		if (this.isClosed()) {
			return;
		}
		this.rollback();
		this.closed = true;
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	@Override
	public ChronoSphereInternal getOwningSphere() {
		return this.owningSphere;
	}

	@Override
	public ChronoGraph getGraph() {
		return this.txGraph;
	}

	@Override
	public ChronoGraphEStore getGraphEStore() {
		return this.graphEStore;
	}

	@Override
	public ChronoEPackageRegistry getEPackageRegistry() {
		this.assertNotClosed();
		return this.ePackageRegistry;
	}

	@Override
	public void batchInsert(final Iterator<EObject> model) {
		checkNotNull(model, "Precondition violation - argument 'model' must not be NULL!");
		this.attach(model, true);
	}

	@Override
	public void reloadEPackageRegistryFromGraph() {
		this.ePackageRegistry = this.owningSphere.getEPackageToGraphMapper()
				.readChronoEPackageRegistryFromGraph(this.txGraph);
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	private void assertNotClosed() {
		if (this.isClosed()) {
			throw new IllegalStateException("This ChronoSphereTransaction was already closed!");
		}
	}

	private ChronoEObjectInternal assertIsChronoEObject(final EObject eObject) {
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		checkArgument(eObject instanceof ChronoEObject,
				"Precondition violation - argument 'eObject' is no ChronoEObject! Did you use the correct EFactory in your EPackage?");
		return (ChronoEObjectInternal) eObject;
	}

	private ChronoEObject createEObjectFromVertex(final Vertex vertex) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		checkArgument(VertexKind.EOBJECT.equals(ChronoSphereGraphFormat.getVertexKind(vertex)),
				"Precondition violation - the given vertex does not represent an EObject!");
		Vertex eClassVertex = Iterators.getOnlyElement(
				vertex.vertices(Direction.OUT, ChronoSphereGraphFormat.createEClassReferenceEdgeLabel()));
		EClass eClass = this.getEPackageRegistry().getEClassByID((String) eClassVertex.id());
		return new ChronoEObjectImpl((String) vertex.id(), eClass, this.getGraphEStore());
	}

	private void attach(final Iterator<? extends EObject> eObjects, final boolean useIncrementalCommits) {
		checkNotNull(eObjects, "Precondition violation - argument 'eObjects' must not be NULL!");
		List<ChronoEObjectInternal> mergeObjects = Lists.newArrayList();
		eObjects.forEachRemaining(eObject -> {
			ChronoEObjectInternal eObjectInternal = this.assertIsChronoEObject(eObject);
			if (eObjectInternal.isAttached()) {
				// already attached, skip it
				return;
			}
			mergeObjects.add(eObjectInternal);
		});
		if (useIncrementalCommits) {
			int batchSize = this.getOwningSphere().getConfiguration().getBatchInsertBatchSize();
			this.getGraphEStore().deepMergeIncremental(mergeObjects, this, batchSize);
		} else {
			this.getGraphEStore().deepMerge(mergeObjects);
		}
	}

}
