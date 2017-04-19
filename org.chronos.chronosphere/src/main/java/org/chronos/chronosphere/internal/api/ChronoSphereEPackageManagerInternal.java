package org.chronos.chronosphere.internal.api;

import org.chronos.chronosphere.api.ChronoSphereEPackageManager;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

public interface ChronoSphereEPackageManagerInternal extends ChronoSphereEPackageManager {

	/**
	 * Overrides the registered {@link EPackage}s with the given ones without touching the corresponding
	 * {@link EObject}s.
	 *
	 * <p>
	 * <u><b>/!\</u> <u>WARNING</u> <u>/!\</b></u><br>
	 * After calling this method, the {@link EObject}s in the model may be <b>out of synch</b> with their corresponding
	 * {@link EClass}es! This method will <b>not</b> take anyprecautions against this problem; it will simply override
	 * the registered EPackages!
	 *
	 *
	 * @param transaction
	 *            The transaction to work on. Must not be <code>null</code>. Will not be committed.
	 * @param newEPackages
	 *            The new EPackages to use for overriding existing ones. Must not be <code>null</code>. If this iterable
	 *            is empty, this method is a no-op.
	 */
	public void overrideEPackages(ChronoSphereTransaction transaction, Iterable<? extends EPackage> newEPackages);
}
