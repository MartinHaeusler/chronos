package org.chronos.chronosphere.internal.ogm.api;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.internal.api.SphereTransactionContext;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EReference;

public interface EObjectToGraphMapper {

	public Vertex getOrCreatePlainVertexForEObject(SphereTransactionContext ctx, ChronoEObject eObject);

	public void mapAllEObjectPropertiesToGraph(SphereTransactionContext ctx, ChronoEObject eObject);

	public void mapAllEObjectReferencesToGraph(SphereTransactionContext ctx, ChronoEObject eObject);

	public void mapEAttributeToGraph(SphereTransactionContext ctx, ChronoEObject eObject, EAttribute attribute);

	public void mapEReferenceToGraph(SphereTransactionContext ctx, ChronoEObject eObject, EReference reference);

	public void mapEContainerReferenceToGraph(SphereTransactionContext ctx, ChronoEObject eObject);

	public void mapEClassReferenceToGraph(SphereTransactionContext ctx, ChronoEObject eObject);

}
