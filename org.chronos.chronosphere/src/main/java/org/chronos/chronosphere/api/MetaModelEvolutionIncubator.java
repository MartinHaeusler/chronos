package org.chronos.chronosphere.api;

import org.chronos.chronosphere.api.exceptions.ElementCannotBeEvolvedException;
import org.chronos.chronosphere.api.exceptions.MetaModelEvolutionCanceledException;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;

public interface MetaModelEvolutionIncubator {

	public EClass migrateClass(EObject oldObject, MetaModelEvolutionContext context)
			throws MetaModelEvolutionCanceledException, ElementCannotBeEvolvedException;

	public void updateAttributeValues(EObject oldObject, EObject newObject, MetaModelEvolutionContext context)
			throws MetaModelEvolutionCanceledException, ElementCannotBeEvolvedException;

	public void updateReferenceTargets(EObject oldObject, EObject newObject, MetaModelEvolutionContext context)
			throws MetaModelEvolutionCanceledException, ElementCannotBeEvolvedException;

}
