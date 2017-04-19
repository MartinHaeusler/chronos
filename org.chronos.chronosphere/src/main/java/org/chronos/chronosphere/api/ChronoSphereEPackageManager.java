package org.chronos.chronosphere.api;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.eclipse.emf.ecore.EPackage;

import com.google.common.collect.Iterators;

public interface ChronoSphereEPackageManager {

	// =================================================================================================================
	// BASIC EPACKAGE MANAGEMENT
	// =================================================================================================================

	public default void registerOrUpdateEPackage(final EPackage ePackage) {
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		this.registerOrUpdateEPackages(Iterators.singletonIterator(ePackage));
	}

	public default void registerOrUpdateEPackage(final EPackage ePackage, final String branchName) {
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.registerOrUpdateEPackages(Iterators.singletonIterator(ePackage), branchName);
	}

	public default void registerOrUpdateEPackages(final Iterable<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		this.registerOrUpdateEPackages(ePackages.iterator());
	}

	public default void registerOrUpdateEPackages(final Iterable<? extends EPackage> ePackages,
			final String branchName) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.registerOrUpdateEPackages(ePackages.iterator(), branchName);
	}

	public default void registerOrUpdateEPackages(final Iterator<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		this.registerOrUpdateEPackages(ePackages, ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	public void registerOrUpdateEPackages(Iterator<? extends EPackage> ePackages, String branchName);

	public default void deleteEPackage(final EPackage ePackage) {
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		this.deleteEPackages(Iterators.singletonIterator(ePackage));
	}

	public default void deleteEPackage(final EPackage ePackage, final String branchName) {
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.deleteEPackages(Iterators.singletonIterator(ePackage), branchName);
	}

	public default void deleteEPackages(final Iterable<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		this.deleteEPackages(ePackages.iterator());
	}

	public default void deleteEPackages(final Iterable<? extends EPackage> ePackages, final String branchName) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.deleteEPackages(ePackages.iterator(), branchName);
	}

	public default void deleteEPackages(final Iterator<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		this.deleteEPackages(ePackages, ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	public void deleteEPackages(final Iterator<? extends EPackage> ePackages, final String branchName);

	public default Set<EPackage> getRegisteredEPackages() {
		return this.getRegisteredEPackages(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	public Set<EPackage> getRegisteredEPackages(String branchName);

	// =================================================================================================================
	// METAMODEL EVOLUTION & INSTANCE ADAPTION
	// =================================================================================================================

	public default void evolveMetamodel(final MetaModelEvolutionIncubator incubator, final EPackage ePackage) {
		checkNotNull(incubator, "Precondition violation - argument 'incubator' must not be NULL!");
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		this.evolveMetamodel(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, incubator, Collections.singleton(ePackage));
	}

	public default void evolveMetamodel(final MetaModelEvolutionController controller, final EPackage ePackage) {
		checkNotNull(controller, "Precondition violation - argument 'controller' must not be NULL!");
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		this.evolveMetamodel(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, controller, Collections.singleton(ePackage));
	}

	public default void evolveMetamodel(final MetaModelEvolutionIncubator incubator,
			final Iterable<? extends EPackage> ePackages) {
		checkNotNull(incubator, "Precondition violation - argument 'incubator' must not be NULL!");
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		this.evolveMetamodel(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, incubator, ePackages);
	}

	public default void evolveMetamodel(final MetaModelEvolutionController controller,
			final Iterable<? extends EPackage> ePackages) {
		checkNotNull(controller, "Precondition violation - argument 'controller' must not be NULL!");
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		this.evolveMetamodel(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, controller, ePackages);
	}

	public default void evolveMetamodel(final String branch, final MetaModelEvolutionIncubator incubator,
			final EPackage ePackage) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(incubator, "Precondition violation - argument 'incubator' must not be NULL!");
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		this.evolveMetamodel(branch, incubator, Collections.singleton(ePackage));
	}

	public default void evolveMetamodel(final String branch, final MetaModelEvolutionController controller,
			final EPackage ePackage) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(controller, "Precondition violation - argument 'controller' must not be NULL!");
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		this.evolveMetamodel(branch, controller, Collections.singleton(ePackage));
	}

	public void evolveMetamodel(final String branch, final MetaModelEvolutionIncubator incubator,
			final Iterable<? extends EPackage> newEPackages);

	public void evolveMetamodel(final String branch, final MetaModelEvolutionController controller,
			final Iterable<? extends EPackage> newEPackages);

}
