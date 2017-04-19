package org.chronos.chronosphere.impl.evolution;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;

import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.api.MetaModelEvolutionContext;
import org.chronos.chronosphere.api.MetaModelEvolutionController;
import org.chronos.chronosphere.api.MetaModelEvolutionIncubator;
import org.chronos.chronosphere.api.exceptions.MetaModelEvolutionCanceledException;
import org.chronos.chronosphere.internal.api.ChronoSphereEPackageManagerInternal;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

public class MetaModelEvolutionProcess {

	public static boolean execute(final ChronoSphere repository, final String branch,
			final MetaModelEvolutionIncubator incubator, final Iterable<? extends EPackage> newEPackages) {
		checkNotNull(repository, "Precondition violation - argument 'repository' must not be NULL!");
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(incubator, "Precondition violation - argument 'incubator' must not be NULL!");
		MetaModelEvolutionController controller = new IncubatorBasedModelEvolutionController(incubator);
		return execute(repository, branch, controller, newEPackages);
	}

	public static boolean execute(final ChronoSphere repository, final String branch,
			final MetaModelEvolutionController controller, final Iterable<? extends EPackage> newEPackages) {
		checkNotNull(repository, "Precondition violation - argument 'repository' must not be NULL!");
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(controller, "Precondition violation - argument 'controller' must not be NULL!");
		// this is going to be our transaction for querying the "old" state
		ChronoSphereTransaction oldTx = repository.tx(branch);
		// this is going to be our transaction for writing and querying the "new" state
		ChronoSphereTransaction newTx = repository.tx(branch);
		try {
			// create the context
			MetaModelEvolutionContext context = new ModelEvolutionContextImpl(repository, branch, oldTx, newTx);

			// create the new epackages in the graph
			((ChronoSphereEPackageManagerInternal) repository.getEPackageManager()).overrideEPackages(newTx,
					newEPackages);
			newTx.commitIncremental();

			// delete all existing EObjects to start from an empty instance graph
			Iterator<EObject> allEObjects = newTx.find().startingFromAllEObjects();
			newTx.delete(allEObjects, false);
			newTx.commitIncremental();

			// invoke the controller
			controller.migrate(context);
			// commit the changes (it's very likely that there have been incremental commits in between)
			newTx.commit();
			// roll back the old transaction
			oldTx.rollback();
			// migration successful
			return true;
		} catch (MetaModelEvolutionCanceledException e) {
			// migration failed
			return false;
		} finally {
			// roll back any uncommitted changes
			if (oldTx != null && oldTx.isOpen()) {
				oldTx.rollback();
			}
			if (newTx != null && newTx.isOpen()) {
				newTx.rollback();
			}
		}
	}

}
