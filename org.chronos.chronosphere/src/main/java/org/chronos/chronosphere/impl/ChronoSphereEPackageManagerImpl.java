package org.chronos.chronosphere.impl;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.api.MetaModelEvolutionController;
import org.chronos.chronosphere.api.MetaModelEvolutionIncubator;
import org.chronos.chronosphere.emf.impl.ChronoEFactory;
import org.chronos.chronosphere.impl.evolution.MetaModelEvolutionProcess;
import org.chronos.chronosphere.internal.api.ChronoSphereEPackageManagerInternal;
import org.chronos.chronosphere.internal.api.ChronoSphereInternal;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.EPackageBundle;
import org.eclipse.emf.ecore.EPackage;

import com.google.common.collect.Lists;

public class ChronoSphereEPackageManagerImpl implements ChronoSphereEPackageManagerInternal {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private ChronoSphereInternal owningSphere;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public ChronoSphereEPackageManagerImpl(final ChronoSphereInternal owningSphere) {
		checkNotNull(owningSphere, "Precondition violation - argument 'owningSphere' must not be NULL!");
		this.owningSphere = owningSphere;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public void registerOrUpdateEPackages(final Iterator<? extends EPackage> ePackages, final String branchName) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");

		// throw the EPackages into a collection
		List<EPackage> packages = Lists.newArrayList();
		ePackages.forEachRemaining(pack -> packages.add(pack));

		EPackageBundle bundle = EPackageBundle.of(packages);

		try (ChronoGraph txGraph = this.owningSphere.getRootGraph().tx().createThreadedTx(branchName)) {
			this.owningSphere.getEPackageToGraphMapper().mapToGraph(txGraph, bundle);
			txGraph.tx().commit();
		}
		// make sure that the EPackage uses the correct EFactory
		packages.forEach(ePackage -> {
			ePackage.setEFactoryInstance(new ChronoEFactory());
		});
	}

	@Override
	public void deleteEPackages(final Iterator<? extends EPackage> ePackages, final String branchName) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");

		// throw the EPackages into a collection
		List<EPackage> packages = Lists.newArrayList();
		ePackages.forEachRemaining(pack -> packages.add(pack));

		EPackageBundle bundle = EPackageBundle.of(packages);

		try (ChronoGraph txGraph = this.owningSphere.getRootGraph().tx().createThreadedTx(branchName)) {
			this.owningSphere.getEPackageToGraphMapper().deleteInGraph(txGraph, bundle);
			txGraph.tx().commit();
		}
	}

	@Override
	public Set<EPackage> getRegisteredEPackages(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(this.owningSphere.getBranchManager().existsBranch(branchName),
				"Precondition violation - argument 'branchName' must refer to an existing branch!");
		try (ChronoGraph txGraph = this.owningSphere.getRootGraph().tx().createThreadedTx(branchName)) {
			ChronoEPackageRegistry registry = this.owningSphere.getEPackageToGraphMapper()
					.readChronoEPackageRegistryFromGraph(txGraph);
			return registry.getEPackages();
		}
	}

	@Override
	public void evolveMetamodel(final String branch, final MetaModelEvolutionController controller,
			final Iterable<? extends EPackage> newEPackages) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(controller, "Precondition violation - argument 'controller' must not be NULL!");
		checkNotNull(newEPackages, "Precondition violation - argument 'newEPackages' must not be NULL!");
		ChronoSphereInternal repo = this.owningSphere;
		MetaModelEvolutionProcess.execute(repo, branch, controller, newEPackages);
	}

	@Override
	public void evolveMetamodel(final String branch, final MetaModelEvolutionIncubator incubator,
			final Iterable<? extends EPackage> newEPackages) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(incubator, "Precondition violation - argument 'incubator' must not be NULL!");
		checkNotNull(newEPackages, "Precondition violation - argument 'newEPackages' must not be NULL!");
		ChronoSphereInternal repo = this.owningSphere;
		MetaModelEvolutionProcess.execute(repo, branch, incubator, newEPackages);
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	@Override
	public void overrideEPackages(final ChronoSphereTransaction transaction,
			final Iterable<? extends EPackage> newEPackages) {
		checkNotNull(transaction, "Precondition violation - argument 'transaction' must not be NULL!");
		checkNotNull(newEPackages, "Precondition violation - argument 'newEPackages' must not be NULL!");
		ChronoSphereTransactionInternal tx = (ChronoSphereTransactionInternal) transaction;
		// get the graph
		ChronoGraph graph = tx.getGraph();
		// create a bundle from the packages
		EPackageBundle bundle = EPackageBundle.of(newEPackages);
		// map them down
		this.owningSphere.getEPackageToGraphMapper().mapToGraph(graph, bundle);
		// reload them in the transaction
		tx.reloadEPackageRegistryFromGraph();
	}

}
