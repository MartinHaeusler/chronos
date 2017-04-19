package org.chronos.chronosphere.internal.ogm.api;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.emf.ecore.EObject;

public interface GraphToEcoreMapper {

	// =====================================================================================================================
	// LOAD METHODS
	// =====================================================================================================================

	public EObject mapVertexToEObject(Vertex vertex);

}
