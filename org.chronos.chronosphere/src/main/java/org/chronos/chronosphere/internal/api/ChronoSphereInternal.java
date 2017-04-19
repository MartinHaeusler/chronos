package org.chronos.chronosphere.internal.api;

import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.internal.ogm.api.EObjectToGraphMapper;
import org.chronos.chronosphere.internal.ogm.api.EPackageToGraphMapper;

public interface ChronoSphereInternal extends ChronoSphere {

	public ChronoGraph getRootGraph();

	public EObjectToGraphMapper getEObjectToGraphMapper();

	public EPackageToGraphMapper getEPackageToGraphMapper();

}
