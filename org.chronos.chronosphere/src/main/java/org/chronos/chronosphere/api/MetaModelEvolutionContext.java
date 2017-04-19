package org.chronos.chronosphere.api;

import java.util.Set;

import org.chronos.chronosphere.api.query.QueryStepBuilderStarter;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

public interface MetaModelEvolutionContext {

	public EPackage getOldEPackage(String namespaceURI);

	public EPackage getNewEPackage(String namespaceURI);

	public Set<EPackage> getOldEPackages();

	public Set<EPackage> getNewEPackages();

	public SphereBranch getMigrationBranch();

	public QueryStepBuilderStarter findInOldModel();

	public QueryStepBuilderStarter findInNewModel();

	public void flush();

	public EObject createAndAttachEvolvedEObject(EObject oldObject, EClass newClass);

	public EObject getCorrespondingEObjectInOldModel(EObject newEObject);

	public EObject getCorrespondingEObjectInNewModel(EObject oldEObject);

	public void deleteInNewModel(EObject newObject);

}
