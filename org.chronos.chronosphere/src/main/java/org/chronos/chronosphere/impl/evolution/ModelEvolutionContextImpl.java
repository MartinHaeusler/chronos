package org.chronos.chronosphere.impl.evolution;

import static com.google.common.base.Preconditions.*;

import java.util.Set;

import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.api.MetaModelEvolutionContext;
import org.chronos.chronosphere.api.SphereBranch;
import org.chronos.chronosphere.api.query.QueryStepBuilderStarter;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

public class ModelEvolutionContextImpl implements MetaModelEvolutionContext {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	protected final ChronoSphere repository;
	protected final SphereBranch branch;
	protected final ChronoSphereTransactionInternal oldTx;
	protected final ChronoSphereTransactionInternal newTx;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public ModelEvolutionContextImpl(final ChronoSphere repository, final String branch,
			final ChronoSphereTransaction oldTx, final ChronoSphereTransaction newTx) {
		checkNotNull(repository, "Precondition violation - argument 'repository' must not be NULL!");
		checkNotNull(oldTx, "Precondition violation - argument 'oldTx' must not be NULL!");
		checkNotNull(newTx, "Precondition violation - argument 'newTx' must not be NULL!");
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		this.repository = repository;
		this.branch = repository.getBranchManager().getBranch(branch);
		this.oldTx = (ChronoSphereTransactionInternal) oldTx;
		this.newTx = (ChronoSphereTransactionInternal) newTx;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public EPackage getOldEPackage(final String namespaceURI) {
		checkNotNull(namespaceURI, "Precondition violation - argument 'namespaceURI' must not be NULL!");
		return this.oldTx.getEPackageByNsURI(namespaceURI);
	}

	@Override
	public EPackage getNewEPackage(final String namespaceURI) {
		checkNotNull(namespaceURI, "Precondition violation - argument 'namespaceURI' must not be NULL!");
		return this.newTx.getEPackageByNsURI(namespaceURI);
	}

	@Override
	public Set<EPackage> getOldEPackages() {
		return this.oldTx.getEPackages();
	}

	@Override
	public Set<EPackage> getNewEPackages() {
		return this.newTx.getEPackages();
	}

	@Override
	public SphereBranch getMigrationBranch() {
		return this.branch;
	}

	@Override
	public QueryStepBuilderStarter findInOldModel() {
		return this.oldTx.find();
	}

	@Override
	public QueryStepBuilderStarter findInNewModel() {
		return this.newTx.find();
	}

	@Override
	public void flush() {
		this.newTx.commitIncremental();
	}

	@Override
	public EObject createAndAttachEvolvedEObject(final EObject oldObject, final EClass newClass) {
		checkNotNull(oldObject, "Precondition violation - argument 'oldObject' must not be NULL!");
		checkNotNull(newClass, "Precondition violation - argument 'newClass' must not be NULL!");
		return this.newTx.createAndAttach(newClass, ((ChronoEObject) oldObject).getId());
	}

	@Override
	public EObject getCorrespondingEObjectInOldModel(final EObject newEObject) {
		checkNotNull(newEObject, "Precondition violation - argument 'newEObject' must not be NULL!");
		return this.oldTx.getEObjectById(((ChronoEObject) newEObject).getId());
	}

	@Override
	public EObject getCorrespondingEObjectInNewModel(final EObject oldEObject) {
		checkNotNull(oldEObject, "Precondition violation - argument 'oldEObject' must not be NULL!");
		return this.newTx.getEObjectById(((ChronoEObject) oldEObject).getId());
	}

	@Override
	public void deleteInNewModel(final EObject newObject) {
		checkNotNull(newObject, "Precondition violation - argument 'newObject' must not be NULL!");
		EObject obj = this.getCorrespondingEObjectInNewModel(newObject);
		if (obj != null) {
			this.newTx.delete(obj);
		}
	}

}