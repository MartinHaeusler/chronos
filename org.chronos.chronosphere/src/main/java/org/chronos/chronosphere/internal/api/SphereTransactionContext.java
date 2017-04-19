package org.chronos.chronosphere.internal.api;

import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;

public interface SphereTransactionContext {

	public ChronoSphereTransactionInternal getTransaction();

	public default ChronoGraph getGraph() {
		return this.getTransaction().getGraph();
	}

	public ChronoEPackageRegistry getChronoEPackage();

}
