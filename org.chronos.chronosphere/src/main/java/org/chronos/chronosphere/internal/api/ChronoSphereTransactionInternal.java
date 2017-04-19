package org.chronos.chronosphere.internal.api;

import java.util.Iterator;

import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.emf.internal.impl.store.ChronoGraphEStore;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;

public interface ChronoSphereTransactionInternal extends ChronoSphereTransaction {

	public EObject createAndAttach(EClass eClass, String eObjectID);

	public ChronoSphereInternal getOwningSphere();

	public ChronoGraph getGraph();

	public ChronoGraphEStore getGraphEStore();

	public ChronoEPackageRegistry getEPackageRegistry();

	public void batchInsert(Iterator<EObject> model);

	public void reloadEPackageRegistryFromGraph();

}
