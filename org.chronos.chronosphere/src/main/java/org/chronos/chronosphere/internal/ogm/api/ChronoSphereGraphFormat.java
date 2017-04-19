package org.chronos.chronosphere.internal.ogm.api;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronosphere.api.exceptions.EObjectPersistenceException;
import org.chronos.chronosphere.api.exceptions.StorageBackendCorruptedException;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.internal.ogm.impl.GremlinUtils;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EEnumLiteral;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class ChronoSphereGraphFormat {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	/** The vertex property that contains the vertex kind. Value is one of the {@link VertexKind} literals. */
	public static final String V_PROP__KIND = "kind";
	/** The common prefix for vertex properties that represent {@link EAttribute} values. */
	public static final String V_PROP_PREFIX__EATTRIBUTE_VALUE = "eAttr_";
	/** The vertex property that contains the {@linkplain EPackage#getNsURI() namespace URI} of an {@link EPackage}. */
	public static final String V_PROP__NS_URI = "nsURI";
	/**
	 * The vertex property that contains the {@linkplain EPackage#getNsPrefix() namespace prefix} of an {@link EPackage}
	 * .
	 */
	public static final String V_PROP__NS_PREFIX = "nsPrefix";
	/**
	 * The vertex property that contains the {@linkplain ENamedElement#getName() name} of {@link EPackage}s,
	 * {@link EClass}es and {@link EStructuralFeature}s.
	 */
	public static final String V_PROP__NAME = "name";
	/**
	 * The vertex property that allows a {@link Vertex} representing an {@link EPackage} to hold the XMI contents of the
	 * EPackage.
	 */
	public static final String V_PROP__XMI_CONTENTS = "xmiContents";
	/** The vertex property that holds the numeric Ecore ID of the {@link EObject#eContainingFeature()}. */
	public static final String V_PROP__ECONTAININGFEATUREID = "eContainingFeatureID";

	/** The edge label that marks the connections between the central EPackage Registry and the registered bundles. */
	public static final String E_LABEL__EPACKAGE_REGISTRY_OWNED_BUNDLE = "ownedBundle";
	/** The edge label that marks the connections between an EPackage Bundle and its contained EPackages. */
	public static final String E_LABEL__BUNDLE_OWNED_EPACKAGE = "ownedEPackage";
	/** The edge label that marks the "eClass" reference. */
	public static final String E_LABEL__ECLASS = "eClass";
	/** The edge label that marks the "eContainer" reference. */
	public static final String E_LABEL__ECONTAINER = "eContainer";
	/** The label for edges that connect an {@link EClass} vertex to the owning {@link EPackage} vertex. */
	public static final String E_LABEL__EPACKAGE_OWNED_CLASSIFIERS = "classifier";
	/** The label for edges that connect an {@link EClass} vertex to an owned {@link EAttribute} vertex. */
	public static final String E_LABEL__ECLASS_OWNED_EATTRIBUTE = "eAttribute";
	/** The label for edges that connect an {@link EClass} vertex to an owned {@link EReference} vertex. */
	public static final String E_LABEL__ECLASS_OWNED_EREFERENCE = "eReference";
	/**
	 * The label for edges that connect an {@link EClass} vertex to one of its {@link EClass#getESuperTypes() supertype}
	 * vertices.
	 */
	public static final String E_LABEL__ECLASS_ESUPERTYPE = "eSuperType";
	/** The label for edges that connect an {@link EPackage} vertex with the vertices representing the sub-EPackages. */
	public static final String E_LABEL__ESUBPACKAGE = "eSubPackage";
	/** The common prefix for labels on edges that represent {@link EReference} links. */
	public static final String E_LABEL_PREFIX__EREFERENCE = "eRef_";
	/** The edge property that contains the ordering for multiplicity-many {@link EReference} links. */
	public static final String E_PROP__ORDER = "eRefOrder";

	public static final String V_ID__EPACKAGE_REGISTRY = "EPackageRegistry_ca68f96b-676c-49de-a260-ac6628a7c455";

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	/**
	 * Returns the {@linkplain VertexKind kind} of the vertex.
	 *
	 * @param vertex
	 *            The vertex to check. Must not be <code>null</code>.
	 * @return The vertex kind, or <code>null</code> if no kind is set.
	 */
	public static VertexKind getVertexKind(final Vertex vertex) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		String vertexKind = (String) vertex.property(V_PROP__KIND).orElse(null);
		return VertexKind.fromString(vertexKind);
	}

	/**
	 * Sets the {@linkplain VertexKind kind} of the vertex.
	 *
	 * @param vertex
	 *            The vertex to assign the new vertex kind to. Must not be <code>null</code>.
	 * @param kind
	 *            The vertex kind to assign to the vertex. Must not be <code>null</code>.
	 */
	public static void setVertexKind(final Vertex vertex, final VertexKind kind) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		checkNotNull(kind, "Precondition violation - argument 'kind' must not be NULL!");
		vertex.property(V_PROP__KIND, kind.toString());
	}

	/**
	 * Creates the {@link Property} key for a {@link Vertex} that needs to store a value for the given
	 * {@link EAttribute}.
	 *
	 * @param registry
	 *            The {@link ChronoEPackageRegistry} to use. Must not be <code>null</code>.
	 * @param eAttribute
	 *            The EAttribute to generate the property key for. Must not be <code>null</code>.
	 * @return The property key. Never <code>null</code>.
	 */
	public static String createVertexPropertyKey(final ChronoEPackageRegistry registry, final EAttribute eAttribute) {
		checkNotNull(registry, "Precondition violation - argument 'registry' must not be NULL!");
		checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
		String featureID = registry.getEAttributeID(eAttribute);
		if (featureID == null) {
			throw new IllegalStateException("Could not generate Vertex Property Key for EAttribute '"
					+ eAttribute.getName() + "'! Did you forget to register or update an EPackage?");
		}
		return V_PROP_PREFIX__EATTRIBUTE_VALUE + featureID;
	}

	/**
	 * Creates the label for an {@link Edge} that represents an instance of the given {@link EReference}.
	 *
	 * @param registry
	 *            The {@link ChronoEPackageRegistry} to use. Must not be <code>null</code>.
	 * @param eReference
	 *            The EReference to generate the edge label for. Must not be <code>null</code>.
	 * @return The edge label. Never <code>null</code>.
	 */
	public static String createReferenceEdgeLabel(final ChronoEPackageRegistry registry, final EReference eReference) {
		checkNotNull(registry, "Precondition violation - argument 'registry' must not be NULL!");
		checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
		String eReferenceID = registry.getEReferenceID(eReference);
		if (eReferenceID == null) {
			throw new IllegalStateException("Could not generate Edge Label for EReference '" + eReference.getName()
					+ "'! Did you forget to register or update an EPackage?");
		}
		return E_LABEL_PREFIX__EREFERENCE + eReferenceID;
	}

	/**
	 * Creates and returns the edge label for the {@link ChronoEObject#eContainer() eContainer} reference edge.
	 *
	 * @return The edge label for the eContainer reference edge. Never <code>null</code>.
	 */
	public static String createEContainerReferenceEdgeLabel() {
		return E_LABEL__ECONTAINER;
	}

	/**
	 * Creates and returns the edge label for the {@link ChronoEObject#eClass() eClass} edge.
	 *
	 * @return The edge label for the eClass edge. Never <code>null</code>.
	 */
	public static String createEClassReferenceEdgeLabel() {
		return E_LABEL__ECLASS;
	}

	/**
	 * Sets the given multiplicity-many {@linkplain EAttribute attribute} value in the given {@link Vertex}.
	 *
	 * @param registry
	 *            The {@link ChronoEPackageRegistry} to use. Must not be <code>null</code>.
	 * @param vertex
	 *            The vertex to write the values to. Must not be <code>null</code>.
	 * @param attribute
	 *            The attribute to write. Must not be <code>null</code>, must be {@linkplain EAttribute#isMany()
	 *            many-valued}.
	 * @param values
	 *            The collection of values to assign to the vertex property. May be <code>null</code> or empty to clear
	 *            the property.
	 * @return The modified property, or <code>null</code> if the property was cleared.
	 */
	public static Property<?> setEAttributeValues(final ChronoEPackageRegistry registry, final Vertex vertex,
			final EAttribute attribute, final Collection<?> values) {
		checkNotNull(registry, "Precondition violation - argument 'registry' must not be NULL!");
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		checkNotNull(attribute, "Precondition violation - argument 'attribute' must not be NULL!");
		checkArgument(attribute.isMany(), "Precondition violation - argument 'attribute' is multiplicity-one!");
		// generate the property key
		String propertyKey = createVertexPropertyKey(registry, attribute);
		if (values == null || values.isEmpty()) {
			// we don't have any values for this attribute; delete the property
			vertex.property(propertyKey).remove();
			return null;
		} else {
			// create a duplicate of the values collection such that we have a "clean" value to persist,
			// i.e. we don't want to store an EList or anything that is a notifier or has some EMF connectoins.
			Collection<?> valueToStore = Lists.newArrayList(values).stream()
					// for each entry, convert it into a persistable form
					.map(value -> convertSingleEAttributeValueToPersistableObject(attribute, value))
					// collect the results in a list
					.collect(Collectors.toList());
			// store the value in the vertex
			vertex.property(propertyKey, valueToStore);
			return vertex.property(propertyKey);
		}
	}

	/**
	 * Returns the values for the given multiplicity-many {@linkplain EAttribute attribute} in the given {@link Vertex}.
	 *
	 * @param registry
	 *            The {@link ChronoEPackageRegistry} to use. Must not be <code>null</code>.
	 * @param vertex
	 *            The vertex to read the value from. Must not be <code>null</code>.
	 * @param attribute
	 *            The attribute to read from the vertex. Must not be <code>null</code>, must be
	 *            {@linkplain EAttribute#isMany() many-valued}.
	 * @return The collection of values assigned to the vertex for the given attribute. May be empty, but never
	 *         <code>null</code>.
	 */
	public static Collection<?> getEAttributeValues(final ChronoEPackageRegistry registry, final Vertex vertex,
			final EAttribute attribute) {
		checkNotNull(registry, "Precondition violation - argument 'registry' must not be NULL!");
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		checkNotNull(attribute, "Precondition violation - argument 'attribute' must not be NULL!");
		checkArgument(attribute.isMany(), "Precondition violation - argument 'attribute' is multiplicity-one!");
		// generate the property key
		String propertyKey = createVertexPropertyKey(registry, attribute);
		// extract the value from the vertex
		Collection<?> storedValue = (Collection<?>) vertex.property(propertyKey).orElse(Lists.newArrayList());
		List<?> resultList = storedValue.stream()
				// for each entry, convert it back from the persistable format into the EObject format
				.map(value -> convertSinglePersistableObjectToEAttributeValue(attribute, attribute))
				// collect the results in a list
				.collect(Collectors.toList());
		// return the result
		return Collections.unmodifiableCollection(resultList);
	}

	/**
	 * Sets the given multiplicity-one {@linkplain EAttribute attribute} value in the given {@link Vertex}.
	 *
	 * @param registry
	 *            The {@link ChronoEPackageRegistry} to use. Must not be <code>null</code>.
	 * @param vertex
	 *            The vertex to write the value to. Must not be <code>null</code>.
	 * @param attribute
	 *            The attribute to write. Must not be <code>null</code>, must not be {@linkplain EAttribute#isMany()
	 *            many-valued}.
	 * @param value
	 *            The value to assign to the vertex property. May be <code>null</code> or empty to clear the property.
	 * @return The modified property, or <code>null</code> if the property was cleared.
	 */
	public static Property<?> setEAttributeValue(final ChronoEPackageRegistry registry, final Vertex vertex,
			final EAttribute attribute, final Object value) {
		checkNotNull(registry, "Precondition violation - argument 'registry' must not be NULL!");
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		checkNotNull(attribute, "Precondition violation - argument 'attribute' must not be NULL!");
		checkArgument(attribute.isMany() == false,
				"Precondition violation - argument 'attribute' is multiplicity-many!");
		// generate the property key
		String propertyKey = createVertexPropertyKey(registry, attribute);
		if (value != null) {
			// store the value in the vertex
			Object persistentValue = convertSingleEAttributeValueToPersistableObject(attribute, value);
			vertex.property(propertyKey, persistentValue);
			return vertex.property(propertyKey);
		} else {
			// no value given; clear the property
			vertex.property(propertyKey).remove();
			return null;
		}
	}

	/**
	 * Returns the value for the given multiplicity-one {@linkplain EAttribute attribute} in the given {@link Vertex}.
	 *
	 * @param registry
	 *            The {@link ChronoEPackageRegistry} to use. Must not be <code>null</code>.
	 * @param vertex
	 *            The vertex to read the value from. Must not be <code>null</code>.
	 * @param attribute
	 *            The attribute to read from the vertex. Must not be <code>null</code>, must not be
	 *            {@linkplain EAttribute#isMany() many-valued}.
	 * @return The value assigned to the vertex for the given attribute. May be <code>null</code> if no value is set on
	 *         the vertex for the given attribute.
	 */
	public static Object getEAttributeValue(final ChronoEPackageRegistry registry, final Vertex vertex,
			final EAttribute attribute) {
		checkNotNull(registry, "Precondition violation - argument 'registry' must not be NULL!");
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		checkNotNull(attribute, "Precondition violation - argument 'attribute' must not be NULL!");
		checkArgument(attribute.isMany() == false,
				"Precondition violation - argument 'attribute' is multiplicity-many!");
		// generate the property key
		String propertyKey = createVertexPropertyKey(registry, attribute);
		// fetch the value
		Object persistentValue = vertex.property(propertyKey).orElse(null);
		return convertSinglePersistableObjectToEAttributeValue(attribute, persistentValue);
	}

	/**
	 * Sets the <code>order</code> property of the given {@link Edge} that represents an {@link EReference} link to the
	 * given value.
	 *
	 * @param edge
	 *            The edge to set the order index for. Must not be <code>null</code>.
	 * @param orderIndex
	 *            The oder index to set. Must not be negative.
	 */
	public static void setEReferenceEdgeOrder(final Edge edge, final int orderIndex) {
		checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
		checkArgument(orderIndex >= 0, "Precondition violation - argument 'orderIndex' must not be negative!");
		edge.property(E_PROP__ORDER, orderIndex);
	}

	/**
	 * Returns the <code>order</code> property of the given {@link Edge} that represents an {@link EReference} link.
	 *
	 * @param edge
	 *            The edge to get the order property for. Must not be <code>null</code>.
	 * @return The order, as an integer. If no order is set, -1 will be returned.
	 */
	public static int getEReferenceEdgeOrder(final Edge edge) {
		checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
		return (int) edge.property(E_PROP__ORDER).orElse(-1);
	}

	/**
	 * Returns the single target of the given {@link EReference} on the given {@link EObject} vertex.
	 *
	 * @param registry
	 *            The {@linkplain ChronoEPackageRegistry package} to work with. Must not be <code>null</code>.
	 * @param eObjectVertex
	 *            The vertex that represents the EObject to get the reference target for. Must not be <code>null</code>.
	 * @param eReference
	 *            The EReference to get the target for. Must not be <code>null</code>. Must not be many-valued.
	 * @return The target vertex, or <code>null</code> if none is set.
	 */
	public static Vertex getEReferenceTarget(final ChronoEPackageRegistry registry, final Vertex eObjectVertex,
			final EReference eReference) {
		checkNotNull(registry, "Precondition violation - argument 'registry' must not be NULL!");
		checkNotNull(eObjectVertex, "Precondition violation - argument 'eObjectVertex' must not be NULL!");
		checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
		checkArgument(eReference.isMany() == false,
				"Precondition violation - argument 'eReference' must not be many-valued!");
		String edgeLabel = createReferenceEdgeLabel(registry, eReference);
		Iterator<Vertex> targets = eObjectVertex.vertices(Direction.OUT, edgeLabel);
		if (targets.hasNext() == false) {
			return null;
		}
		Vertex target = targets.next();
		if (targets.hasNext()) {
			throw new IllegalStateException("Found multiple targets for EObject '" + eObjectVertex.id() + "#"
					+ eReference.getName() + " (multiplicity one)!");
		}
		return target;
	}

	/**
	 * Returns the targets of the given {@link EReference} on the given {@link EObject} vertex.
	 *
	 * @param registry
	 *            The {@linkplain ChronoEPackageRegistry package} to work with. Must not be <code>null</code>.
	 * @param eObjectVertex
	 *            The vertex that represents the EObject to get the reference target for. Must not be <code>null</code>.
	 * @param eReference
	 *            The EReference to get the target for. Must not be <code>null</code>. Must be many-valued.
	 * @return The target vertices (in the correct order), or <code>null</code> if none is set.
	 */
	public static List<Vertex> getEReferenceTargets(final ChronoEPackageRegistry registry, final Vertex eObjectVertex,
			final EReference eReference) {
		checkNotNull(registry, "Precondition violation - argument 'registry' must not be NULL!");
		checkNotNull(eObjectVertex, "Precondition violation - argument 'eObjectVertex' must not be NULL!");
		checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
		checkArgument(eReference.isMany(), "Precondition violation - argument 'eReference' must be many-valued!");
		String edgeLabel = createReferenceEdgeLabel(registry, eReference);
		// get the reference edges (as they contain the ordering)
		List<Edge> edges = Lists.newArrayList(eObjectVertex.edges(Direction.OUT, edgeLabel));
		// sort the edges by their ordering
		edges.sort((e1, e2) -> {
			int order1 = getEReferenceEdgeOrder(e1);
			int order2 = getEReferenceEdgeOrder(e2);
			return Integer.compare(order1, order2);
		});
		// for each edge, get the target vertex
		return edges.stream().map(edge -> edge.inVertex()).collect(Collectors.toList());
	}

	/**
	 * Sets the target of the given {@link EReference} on the given {@link EObject} vertex to the given target vertex.
	 *
	 * @param registry
	 *            The {@linkplain ChronoEPackageRegistry package} to work with. Must not be <code>null</code>.
	 * @param eObjectVertex
	 *            The vertex representing the EObject to change the reference target for (i.e. the reference owner).
	 *            Must not be <code>null</code>.
	 * @param eReference
	 *            The EReference to set. Must not be <code>null</code>, must not be many-valued.
	 * @param target
	 *            The vertex representing the target EObject. May be <code>null</code> to clear the reference.
	 */
	public static void setEReferenceTarget(final ChronoEPackageRegistry registry, final Vertex eObjectVertex,
			final EReference eReference, final Vertex target) {
		checkNotNull(registry, "Precondition violation - argument 'registry' must not be NULL!");
		checkNotNull(eObjectVertex, "Precondition violation - argument 'eObjectVertex' must not be NULL!");
		checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
		checkArgument(eReference.isMany() == false,
				"Precondition violation - argument 'eReference' must not be many-valued!");
		String edgeLabel = createReferenceEdgeLabel(registry, eReference);
		if (target == null) {
			// remove the edge(s)
			eObjectVertex.edges(Direction.OUT, edgeLabel).forEachRemaining(edge -> edge.remove());
		} else {
			// set the edges
			GremlinUtils.setEdgeTarget(eObjectVertex, edgeLabel, target);
		}
	}

	/**
	 * Sets the target of the given {@link EReference} on the given {@link EObject} vertex to the given target vertices.
	 *
	 * @param registry
	 *            The {@linkplain ChronoEPackageRegistry package} to work with. Must not be <code>null</code>.
	 * @param eObjectVertex
	 *            The vertex representing the EObject to change the reference target for (i.e. the reference owner).
	 *            Must not be <code>null</code>.
	 * @param eReference
	 *            The EReference to set. Must not be <code>null</code>, must be many-valued.
	 * @param targets
	 *            The vertices representing the target EObjects. May be <code>null</code> or empty to clear the
	 *            reference.
	 */
	public static void setEReferenceTargets(final ChronoEPackageRegistry registry, final Vertex eObjectVertex,
			final EReference eReference, final List<Vertex> targets) {
		checkNotNull(registry, "Precondition violation - argument 'registry' must not be NULL!");
		checkNotNull(eObjectVertex, "Precondition violation - argument 'eObjectVertex' must not be NULL!");
		checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
		checkArgument(eReference.isMany(), "Precondition violation - argument 'eReference' must be many-valued!");
		String edgeLabel = createReferenceEdgeLabel(registry, eReference);
		if (targets == null || targets.isEmpty()) {
			// remove the edge(s)
			eObjectVertex.edges(Direction.OUT, edgeLabel).forEachRemaining(edge -> edge.remove());
		} else {
			// set the targets
			List<Edge> edges = GremlinUtils.setEdgeTargets(eObjectVertex, edgeLabel, targets);
			int order = 0;
			for (Edge edge : edges) {
				setEReferenceEdgeOrder(edge, order);
				order++;
			}
		}
	}

	/**
	 * Returns the {@link Vertex} that represents the given {@link EClass} in the given {@link ChronoGraph graph}.
	 *
	 * @param registry
	 *            The {@link ChronoEPackageRegistry} to use. Must not be <code>null</code>.
	 * @param graph
	 *            The graph to search in. Must not be <code>null</code>.
	 * @param eClass
	 *            The EClass to get the vertex for. Must not be <code>null</code>.
	 * @return The vertex that represents the given EClass, or <code>null</code> if there is no such vertex.
	 */
	public static Vertex getVertexForEClass(final ChronoEPackageRegistry registry, final ChronoGraph graph,
			final EClass eClass) {
		checkNotNull(registry, "Precondition violation - argument 'registry' must not be NULL!");
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		String id = registry.getEClassID(eClass);
		if (id == null) {
			throw new IllegalStateException(
					"There is no ID for EClass '" + eClass.getName() + "'! Did you forget to register an EPackage?");
		}
		Iterator<Vertex> vertices = graph.vertices(id);
		if (vertices == null || vertices.hasNext() == false) {
			return null;
		}
		return Iterators.getOnlyElement(vertices);
	}

	/**
	 * Attempts to find the {@link Vertex} that represents the given {@link EClass} in the graph.
	 *
	 * <p>
	 * In contrast to {@link #getVertexForEClass(ChronoEPackageRegistry, ChronoGraph, EClass)}, this method does not
	 * rely on IDs, but rather on the graph structure itself and on the name of the {@link EClass}.
	 *
	 * <p>
	 * This method should <b>not</b> be used outside of a package initialization process!
	 *
	 * @param graph
	 *            The graph to search in. Must not be <code>null</code>.
	 * @param eClass
	 *            The EClass to search the vertex for. Must not be <code>null</code>.
	 *
	 * @return The vertex that represents the given EClass, or <code>null</code> if no vertex is found.
	 */
	public static Vertex getVertexForEClassRaw(final ChronoGraph graph, final EClass eClass) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		Vertex ePackageVertex = getVertexForEPackage(graph, eClass.getEPackage());
		if (ePackageVertex == null) {
			// ePackage is not mapped -> eClass can't be mapped
			return null;
		}
		Iterator<Vertex> eClassVertices = graph.traversal()
				// start at the ePackage
				.V(ePackageVertex)
				// follow the "owned classifiers" edges
				.out(E_LABEL__EPACKAGE_OWNED_CLASSIFIERS)
				// filter only EClass vertices
				.has(V_PROP__KIND, VertexKind.ECLASS.toString())
				// filter the vertices by the name of the EClass
				.has(V_PROP__NAME, eClass.getName());
		if (eClassVertices.hasNext() == false) {
			// no result found, eClass is not mapped
			return null;
		}
		return Iterators.getOnlyElement(eClassVertices);
	}

	/**
	 * Returns the {@linkplain Vertex vertex} from the given {@linkplain ChronoGraph graph} that represents the given
	 * {@linkplain ChronoEObject EObject}.
	 *
	 * @param graph
	 *            The graph to search in. Must not be <code>null</code>.
	 * @param eObject
	 *            The EObject to search the vertex for. Must not be <code>null</code>.
	 * @return The vertex for the EObject, or <code>null</code> if none was found.
	 */
	public static Vertex getVertexForEObject(final ChronoGraph graph, final ChronoEObject eObject) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		Iterator<Vertex> vertices = graph.vertices(eObject.getId());
		if (vertices == null || vertices.hasNext() == false) {
			return null;
		}
		return Iterators.getOnlyElement(vertices);
	}

	/**
	 * Returns the {@link Vertex} in the given {@link ChronoGraph} that represents the {@link ChronoEObject} with the
	 * given ID.
	 *
	 * @param graph
	 *            The graph to search in. Must not be <code>null</code>.
	 * @param eObjectId
	 *            The ID of the ChronoEObject to search for. Must not be <code>null</code>.
	 * @return The vertex, or <code>null</code> if there is no ChronoEObject in the graph with the given ID.
	 */
	public static Vertex getVertexForEObject(final ChronoGraph graph, final String eObjectId) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(eObjectId, "Precondition violation - argument 'eObjectId' must not be NULL!");
		Iterator<Vertex> iterator = graph.vertices(eObjectId);
		if (iterator == null || iterator.hasNext() == false) {
			return null;
		}
		return Iterators.getOnlyElement(iterator);
	}

	private static Vertex getOrCreateEPackageRegistryVertex(final ChronoGraph graph) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		Iterator<Vertex> vertices = graph.vertices(V_ID__EPACKAGE_REGISTRY);
		if (vertices == null || vertices.hasNext() == false) {
			// create the root epackage registry vertex
			Vertex vertex = graph.addVertex(T.id, V_ID__EPACKAGE_REGISTRY);
			setVertexKind(vertex, VertexKind.EPACKAGE_REGISTRY);
			return vertex;
		} else {
			return Iterators.getOnlyElement(vertices);
		}
	}

	/**
	 * Returns the {@link Vertex} that represents the given {@link EPackage}.
	 *
	 * @param graph
	 *            The graph to search in. Must not be <code>null</code>.
	 * @param ePackage
	 *            The ePackage to get the vertex for. Must not be <code>null</code>.
	 * @return The vertex, or <code>null</code> if none exists.
	 */
	public static Vertex getVertexForEPackage(final ChronoGraph graph, final EPackage ePackage) {
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		Vertex ePackageRegistryVertex = getOrCreateEPackageRegistryVertex(graph);
		Iterator<Vertex> vertices = graph.traversal()
				// ePackages are reflected in the graph as vertices. Start at the EPackage Registry vertex
				.V(ePackageRegistryVertex)
				// go to the bundles
				.out(E_LABEL__EPACKAGE_REGISTRY_OWNED_BUNDLE)
				// go to the EPackages
				.out(E_LABEL__BUNDLE_OWNED_EPACKAGE)
				// we are only interested in EPackages
				.has(V_PROP__KIND, VertexKind.EPACKAGE.toString())
				// the namespace URI and the namespace prefix must match
				.has(V_PROP__NS_URI, ePackage.getNsURI()).has(V_PROP__NS_PREFIX, ePackage.getNsPrefix())
				// the epackage name must also match
				.has(V_PROP__NAME, ePackage.getName());
		if (vertices.hasNext() == false) {
			// no EPackage vertex found
			return null;
		}
		// we should have exactly one vertex now
		return Iterators.getOnlyElement(vertices);
	}

	/**
	 * Creates a vertex in the given graph for the given {@link EPackage}.
	 *
	 * <p>
	 * This method will also create the corresponding edges and set the appropriate vertex properties on the newly
	 * created vertex.
	 *
	 * @param graph
	 *            The graph to work on. Must not be <code>null</code>.
	 * @param ePackage
	 *            The EPackage to create the vertex for. Must not be <code>null</code>. Must not exist in the graph yet.
	 * @return The newly created vertex. Never <code>null</code>.
	 * @throws IllegalStateException
	 *             Thrown if there is already a vertex in the graph for the given EPackage.
	 */
	public static Vertex createVertexForEPackage(final ChronoGraph graph, final EPackage ePackage) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		if (getVertexForEPackage(graph, ePackage) != null) {
			throw new IllegalStateException("There already is a vertex for Root EPackage '" + ePackage.getName()
					+ "' (URI: '" + ePackage.getNsURI() + "')!");
		}
		Vertex ePackageVertex = graph.addVertex(T.id, UUID.randomUUID().toString());
		setVertexKind(ePackageVertex, VertexKind.EPACKAGE);
		setVertexName(ePackageVertex, ePackage.getName());
		setNsURI(ePackageVertex, ePackage.getNsURI());
		setNsPrefix(ePackageVertex, ePackage.getNsPrefix());
		if (ePackage.getESuperPackage() != null) {
			// we are dealing with a sub-package; create an edge to the parent package
			Vertex superPackageVertex = getVertexForEPackage(graph, ePackage.getESuperPackage());
			if (superPackageVertex == null) {
				throw new IllegalArgumentException("EPackage '" + ePackage.getName() + "' (URI: '" + ePackage.getNsURI()
						+ "') has no Vertex representation of its super EPackage!");
			}
			superPackageVertex.addEdge(E_LABEL__ESUBPACKAGE, ePackageVertex);
		}
		return ePackageVertex;
	}

	/**
	 * Creates a new {@link Vertex} for the given {@link EPackageBundle}.
	 *
	 * @param graph
	 *            The graph to modify. Must not be <code>null</code>.
	 * @param bundle
	 *            The {@link EPackageBundle} to represent in the graph. Must not be <code>null</code>.
	 * @return The newly created vertex representing the bundle. Never <code>null</code>.
	 */
	public static Vertex createVertexForEPackageBundle(final ChronoGraph graph, final EPackageBundle bundle) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(bundle, "Precondition violation - argument 'bundle' must not be NULL!");
		Vertex bundleVertex = graph.addVertex();
		setVertexKind(bundleVertex, VertexKind.EPACKAGE_BUNDLE);
		String xmi = EMFUtils.writeEPackagesToXMI(bundle);
		setXMIContents(bundleVertex, xmi);
		Vertex ePackageRegistryVertex = getOrCreateEPackageRegistryVertex(graph);
		ePackageRegistryVertex.addEdge(E_LABEL__EPACKAGE_REGISTRY_OWNED_BUNDLE, bundleVertex);
		return bundleVertex;
	}

	/**
	 * Updates the contents of the {@link Vertex} to reflect the given {@link EPackageBundle}.
	 *
	 * @param bundleVertex
	 *            The bundle vertex to update. Must not be <code>null</code>.
	 * @param bundle
	 *            The {@link EPackageBundle} to get the data from. Must not be <code>null</code>.
	 */
	public static void updateEPackageBundleVertex(final Vertex bundleVertex, final EPackageBundle bundle) {
		checkNotNull(bundleVertex, "Precondition violation - argument 'bundleVertex' must not be NULL!");
		checkNotNull(bundle, "Precondition violation - argument 'bundle' must not be NULL!");
		checkArgument(VertexKind.EPACKAGE_BUNDLE.equals(getVertexKind(bundleVertex)),
				"Precondition violation - argument 'bundleVertex' is not an EPackageBundle vertex!");
		String xmi = EMFUtils.writeEPackagesToXMI(bundle);
		setXMIContents(bundleVertex, xmi);
	}

	/**
	 * Sets the name of the given vertex to the given value.
	 *
	 * <p>
	 * Please note that not all vertices are meant to have a name. In particular, regular {@link EObject} vertices don't
	 * use this property. It is meant primarily for meta-elements, such as vertices that represent {@link EPackage}s,
	 * {@link EClass}es, {@link EAttribute}s and {@link EReference}s.
	 *
	 * @param vertex
	 *            The vertex to set the name for. Must not be <code>null</code>.
	 * @param name
	 *            The name to assign to the vertex. May be <code>null</code> to clear the name.
	 */
	public static void setVertexName(final Vertex vertex, final String name) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		if (name == null) {
			// clear the name property
			vertex.property(V_PROP__NAME).remove();
		} else {
			// set the name property
			vertex.property(V_PROP__NAME, name);
		}
	}

	/**
	 * Sets the {@link EPackage#getNsURI() namespace URI} for the given {@link EPackage} vertex.
	 *
	 * @param ePackageVertex
	 *            The vertex to modify. Must not be <code>null</code>.
	 * @param nsURI
	 *            The namespace URI to set. May be <code>null</code> to clear the namespace URI property.
	 */
	public static void setNsURI(final Vertex ePackageVertex, final String nsURI) {
		checkNotNull(ePackageVertex, "Precondition violation - argument 'ePackageVertex' must not be NULL!");
		if (nsURI == null) {
			ePackageVertex.property(V_PROP__NS_URI).remove();
		} else {
			ePackageVertex.property(V_PROP__NS_URI, nsURI);
		}
	}

	/**
	 * Returns the {@link EPackage#getNsURI() namespace URI} stored in the given {@link EPackage} vertex.
	 *
	 * @param ePackageVertex
	 *            The vertex to read the namespace URI from. Must not be <code>null</code>.
	 * @return The namespace URI, or <code>null</code> if none is set.
	 */
	public static String getNsURI(final Vertex ePackageVertex) {
		checkNotNull(ePackageVertex, "Precondition violation - argument 'ePackageVertex' must not be NULL!");
		return (String) ePackageVertex.property(V_PROP__NS_URI).orElse(null);
	}

	/**
	 * Sets the {@link EPackage#getNsPrefix() namespace prefix} for the given {@link EPackage} vertex.
	 *
	 * @param ePackageVertex
	 *            The vertex to modify. Must not be <code>null</code>.
	 * @param nsPrefix
	 *            The namespace prefix to set. May be <code>null</code> to clear the namespace Prefix property.
	 */
	public static void setNsPrefix(final Vertex ePackageVertex, final String nsPrefix) {
		checkNotNull(ePackageVertex, "Precondition violation - argument 'ePackageVertex' must not be NULL!");
		if (nsPrefix == null) {
			ePackageVertex.property(V_PROP__NS_PREFIX).remove();
		} else {
			ePackageVertex.property(V_PROP__NS_PREFIX, nsPrefix);
		}
	}

	/**
	 * Returns the {@link EPackage#getNsPrefix() namespace Prefix} stored in the given {@link EPackage} vertex.
	 *
	 * @param ePackageVertex
	 *            The vertex to read the namespace Prefix from. Must not be <code>null</code>.
	 * @return The namespace Prefix, or <code>null</code> if none is set.
	 */
	public static String getNsPrefix(final Vertex ePackageVertex) {
		checkNotNull(ePackageVertex, "Precondition violation - argument 'ePackageVertex' must not be NULL!");
		return (String) ePackageVertex.property(V_PROP__NS_PREFIX).orElse(null);
	}

	/**
	 * Returns the name of the given vertex.
	 *
	 * <p>
	 * Please note that not all vertices are meant to have a name. In particular, regular {@link EObject} vertices don't
	 * use this property. It is meant primarily for meta-elements, such as vertices that represent {@link EPackage}s,
	 * {@link EClass}es, {@link EAttribute}s and {@link EReference}s.
	 *
	 * @param vertex
	 *            The vertex to return the name property for. Must not be <code>null</code>.
	 * @return The name, or <code>null</code> if the given vertex doesn't have a name assigned.
	 */
	public static String getVertexName(final Vertex vertex) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		return (String) vertex.property(V_PROP__NAME).orElse(null);
	}

	/**
	 * Sets the XMI contents of the given {@link Vertex}.
	 *
	 * <p>
	 * Please note that this is intended exclusively for vertices that represent {@link EPackage}s.
	 *
	 * @param vertex
	 *            The vertex to set the XMI contents for. Must not be <code>null</code>.
	 * @param xmiContents
	 *            The XMI contents to store in the vertex. May be <code>null</code> to clear.
	 */
	public static void setXMIContents(final Vertex vertex, final String xmiContents) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		if (xmiContents == null || xmiContents.trim().isEmpty()) {
			// clear the property
			vertex.property(V_PROP__XMI_CONTENTS).remove();
		} else {
			// store the XMI contents
			vertex.property(V_PROP__XMI_CONTENTS, xmiContents);
		}
	}

	/**
	 * Returns the XMI contents stored in the given {@link Vertex}.
	 *
	 * <p>
	 * Please note that this is intended exclusively for vertices that represent {@link EPackage}s.
	 *
	 * @param vertex
	 *            The vertex to get the XMI data from. Must not be <code>null</code>.
	 * @return The XMI data stored in the vertex, or <code>null</code> if the vertex contains no XMI data.
	 */
	public static String getXMIContents(final Vertex vertex) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		return (String) vertex.property(V_PROP__XMI_CONTENTS).orElse(null);
	}

	/**
	 * Returns the {@link Vertex} that represents the given {@link EAttribute} and is attached to the given
	 * {@link EClass} vertex.
	 *
	 * @param eClassVertex
	 *            The vertex that represents the EClass that owns the EAttribute. Must not be <code>null</code>.
	 * @param eAttribute
	 *            The EAttribute to get the vertex for. Must not be <code>null</code>.
	 * @return The vertex that represents the given EAttribute within the given EClass. May be <code>null</code> if no
	 *         vertex for the given EAttribute was found.
	 */
	public static Vertex getVertexForEAttributeRaw(final Vertex eClassVertex, final EAttribute eAttribute) {
		checkNotNull(eClassVertex, "Precondition violation - argument 'eClassVertex' must not be NULL!");
		checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
		Graph graph = eClassVertex.graph();
		Iterator<Vertex> eAttributeVertices = graph.traversal()
				// start with the eClass vertex
				.V(eClassVertex)
				// follow the "owned eAttribute" label
				.out(E_LABEL__ECLASS_OWNED_EATTRIBUTE)
				// filter only EAttributes
				.has(V_PROP__KIND, VertexKind.EATTRIBUTE.toString())
				// filter by name
				.has(V_PROP__NAME, eAttribute.getName());
		if (eAttributeVertices.hasNext() == false) {
			// we did not find a vertex that represents the given EAttribute
			return null;
		}
		return Iterators.getOnlyElement(eAttributeVertices);
	}

	/**
	 * Returns the {@link Vertex} that represents the given {@link EReference} and is attached to the given
	 * {@link EClass} vertex.
	 *
	 * @param eClassVertex
	 *            The vertex that represents the EClass that owns the EAttribute. Must not be <code>null</code>.
	 * @param eReference
	 *            The EReference to get the vertex for. Must not be <code>null</code>.
	 * @return The vertex that represents the given EReference within the given EClass. May be <code>null</code> if no
	 *         vertex for the given EReference was found.
	 */
	public static Vertex getVertexForEReferenceRaw(final Vertex eClassVertex, final EReference eReference) {
		checkNotNull(eClassVertex, "Precondition violation - argument 'eClassVertex' must not be NULL!");
		checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
		Graph graph = eClassVertex.graph();
		Iterator<Vertex> eReferenceVertices = graph.traversal()
				// start with the eClass vertex
				.V(eClassVertex)
				// follow the "owned eReference" label
				.out(E_LABEL__ECLASS_OWNED_EREFERENCE)
				// filter only EReferences
				.has(V_PROP__KIND, VertexKind.EREFERENCE.toString())
				// filter by name
				.has(V_PROP__NAME, eReference.getName());
		if (eReferenceVertices.hasNext() == false) {
			// we did not find a vertex that represents the given EReference
			return null;
		}
		return Iterators.getOnlyElement(eReferenceVertices);
	}

	/**
	 * Returns the {@link EClass} that acts as the classifier for the {@link EObject} represented by the given
	 * {@link Vertex}.
	 *
	 * @param registry
	 *            The {@linkplain ChronoEPackageRegistry package} to work with. Must not be <code>null</code>.
	 * @param vertex
	 *            The vertex representing the EObject to get the EClass for. Must not be <code>null</code>.
	 * @return The EClass, or <code>null</code> if none is found.
	 */
	public static EClass getEClassForEObjectVertex(final ChronoEPackageRegistry registry, final Vertex vertex) {
		checkNotNull(registry, "Precondition violation - argument 'registry' must not be NULL!");
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		String label = createEClassReferenceEdgeLabel();
		Iterator<Vertex> classifierVertices = vertex.vertices(Direction.OUT, label);
		Vertex eClassVertex = Iterators.getOnlyElement(classifierVertices, null);
		if (eClassVertex == null) {
			return null;
		}
		String eClassID = (String) eClassVertex.id();
		EClass eClass = registry.getEClassByID(eClassID);
		return eClass;
	}

	/**
	 * Sets the EClass for the EObject represented by the given vertex.
	 *
	 * @param registry
	 *            The {@linkplain ChronoEPackageRegistry package} to work with. Must not be <code>null</code>.
	 * @param vertex
	 *            The vertex representing the EObject to set the EClass for. Must not be <code>null</code>.
	 * @param eClass
	 *            The eClass to use. Must be part of the given package. Must not be <code>null</code>.
	 */
	public static void setEClassForEObjectVertex(final ChronoEPackageRegistry registry, final Vertex vertex,
			final EClass eClass) {
		checkNotNull(registry, "Precondition violation - argument 'registry' must not be NULL!");
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		ChronoGraph graph = (ChronoGraph) vertex.graph();
		Vertex eClassVertex = getVertexForEClass(registry, graph, eClass);
		String label = createEClassReferenceEdgeLabel();
		GremlinUtils.setEdgeTarget(vertex, label, eClassVertex);
	}

	/**
	 * Sets the {@link EObject#eContainer() eContainer()} of the {@link EObject} represented by the given source vertex
	 * to the EObject represented by the given target vertex.
	 *
	 * @param sourceVertex
	 *            The source vertex that should be relocated to the new container. Must not be <code>null</code>.
	 * @param targetVertex
	 *            The target vertex that should act as the new container. May be <code>null</code> to clear the
	 *            eContainer of the source vertex.
	 */
	public static void setEContainer(final Vertex sourceVertex, final Vertex targetVertex) {
		checkNotNull(sourceVertex, "Precondition violation - argument 'sourceVertex' must not be NULL!");
		if (targetVertex == null) {
			sourceVertex.edges(Direction.OUT, E_LABEL__ECONTAINER).forEachRemaining(edge -> edge.remove());
		} else {
			GremlinUtils.setEdgeTarget(sourceVertex, E_LABEL__ECONTAINER, targetVertex);
		}
	}

	/**
	 * Returns the vertex representing the {@link EObject} that acts as the {@link EObject#eContainer() eContainer()} of
	 * the given EObject.
	 *
	 * @param eObjectVertex
	 *            The vertex that represents the eObject to get the eContainer for. Must not be <code>null</code>.
	 * @return The vertex representing the EObject that is the eContainer of the given eObject. May be <code>null</code>
	 *         if no eContainer is present.
	 */
	public static Vertex getEContainer(final Vertex eObjectVertex) {
		checkNotNull(eObjectVertex, "Precondition violation - argument 'eObjectVertex' must not be NULL!");
		return Iterators.getOnlyElement(eObjectVertex.vertices(Direction.OUT, E_LABEL__ECONTAINER), null);
	}

	/**
	 * Sets the numeric Ecore ID of the {@linkplain EObject#eContainingFeature() eContainingFeature} on the EObject
	 * represented by the given vertex.
	 *
	 * @param eObjectVertex
	 *            The vertex representing the EObject where the eContainingFeatureID should be changed. Must not be
	 *            <code>null</code>.
	 * @param containingFeatureId
	 *            The new eContainingFeatureID to use. May be <code>null</code> to clear it.
	 */
	public static void setEContainingFeatureId(final Vertex eObjectVertex, final Integer containingFeatureId) {
		checkNotNull(eObjectVertex, "Precondition violation - argument 'eObjectVertex' must not be NULL!");
		if (containingFeatureId == null) {
			eObjectVertex.property(V_PROP__ECONTAININGFEATUREID).remove();
		} else {
			eObjectVertex.property(V_PROP__ECONTAININGFEATUREID, containingFeatureId);
		}
	}

	/**
	 * Returns the numeric Ecore ID of the {@linkplain EObject#eContainingFeature() eContainingFeature} on the EObject
	 * represented by the given vertex.
	 *
	 * @param eObjectVertex
	 *            The vertex representing the EObject where the eContainingFeatureID should be retrieved. Must not be
	 *            <code>null</code>.
	 * @return The eContainingFeatureID (which may be negative according to the Ecore standard), or <code>null</code> if
	 *         no eContainingFeatureID is set.
	 */
	public static Integer getEContainingFeatureId(final Vertex eObjectVertex) {
		checkNotNull(eObjectVertex, "Precondition violation - argument 'eObjectVertex' must not be NULL!");
		return (Integer) eObjectVertex.property(V_PROP__ECONTAININGFEATUREID).orElse(null);
	}

	/**
	 * Returns the {@link Vertex} that represents the root of the {@link EPackage} hierarchy that contains the
	 * {@link EClass} represented by the given vertex.
	 *
	 * @param eClassVertex
	 *            The vertex that represents the EClass. Must not be <code>null</code>.
	 *
	 * @return The vertex that represents the root EPackage that contains the given EPackage vertex. Never
	 *         <code>null</code>.
	 */
	public static Vertex getRootEPackageVertexForEClassVertex(final Vertex eClassVertex) {
		checkNotNull(eClassVertex, "Precondition violation - argument 'ePackageVertex' must not be NULL!");
		checkArgument(VertexKind.ECLASS.equals(getVertexKind(eClassVertex)),
				"Precondition violation - argument 'ePackageVertex' must have a VertexKind of ECLASS!");
		Vertex ePackageVertex = getEPackageVertexForEClassVertex(eClassVertex);
		return getRootEPackageVertexForEPackageVertex(ePackageVertex);
	}

	/**
	 * Returns the {@link Vertex} that represents the root of the {@link EPackage} hierarchy that contains the
	 * {@link EPackage} represented by the given vertex.
	 *
	 * @param ePackageVertex
	 *            The vertex that represents the EPackage. Must not be <code>null</code>.
	 *
	 * @return The vertex that represents the root EPackage that contains the given EPackage vertex. Never
	 *         <code>null</code>.
	 */
	public static Vertex getRootEPackageVertexForEPackageVertex(final Vertex ePackageVertex) {
		checkNotNull(ePackageVertex, "Precondition violation - argument 'ePackageVertex' must not be NULL!");
		checkArgument(VertexKind.EPACKAGE.equals(getVertexKind(ePackageVertex)),
				"Precondition violation - argument 'ePackageVertex' must have a VertexKind of EPACKAGE!");
		Vertex v = ePackageVertex;
		// navigate upwards the containment hierarchy to find the root EPackage
		Iterator<Vertex> superPackageVertices = v.vertices(Direction.IN, E_LABEL__ESUBPACKAGE);
		while (superPackageVertices.hasNext()) {
			v = Iterators.getOnlyElement(superPackageVertices);
			superPackageVertices = v.vertices(Direction.IN, E_LABEL__ESUBPACKAGE);
		}
		return v;
	}

	/**
	 * Returns the {@link Vertex} that represents the {@link EPackage} that contains the {@link EClass} represented by
	 * the given vertex.
	 *
	 * @param eClassVertex
	 *            The vertex that represents the EClass. Must not be <code>null</code>.
	 *
	 * @return The vertex that represents the root EPackage that contains the given EClass vertex. Never
	 *         <code>null</code>.
	 */
	public static Vertex getEPackageVertexForEClassVertex(final Vertex eClassVertex) {
		checkNotNull(eClassVertex, "Precondition violation - argument 'eClassVertex' must not be NULL!");
		checkArgument(VertexKind.ECLASS.equals(getVertexKind(eClassVertex)),
				"Precondition violation - argument 'eClassVertex' must have a VertexKind of ECLASS!");
		Iterator<Vertex> owningEPackageVertices = eClassVertex.vertices(Direction.IN,
				E_LABEL__EPACKAGE_OWNED_CLASSIFIERS);
		if (owningEPackageVertices.hasNext() == false) {
			throw new StorageBackendCorruptedException(
					"The vertex that represents EClass " + getVertexName(eClassVertex) + " has no owning EPackage!");
		}
		// get the EPackage vertex (may be a sub-package)
		Vertex ePackageVertex = Iterators.getOnlyElement(owningEPackageVertices);
		return ePackageVertex;
	}

	/**
	 * Returns the set of {@linkplain Vertex vertices} that represent {@link EPackageBundle}s.
	 *
	 * @param graph
	 *            The graph to search in. Must not be <code>null</code>.
	 *
	 * @return The set of vertices that represent the mapped bundles. May be empty, but never <code>null</code>.
	 */
	public static Set<Vertex> getEpackageBundleVertices(final ChronoGraph graph) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		Set<Vertex> vertices = graph.traversal()
				// start with all vertices
				.V()
				// limit to the ones that have 'kind' equal to 'bundle'
				.has(V_PROP__KIND, VertexKind.EPACKAGE_BUNDLE.toString())
				// put result into a set
				.toSet();
		return vertices;
	}

	// =====================================================================================================================
	// HELPER METHODS
	// =====================================================================================================================

	private static Object convertSingleEAttributeValueToPersistableObject(final EAttribute eAttribute,
			final Object value) {
		if (value instanceof EEnumLiteral) {
			// for EEnumLiterals, we store the 'literal' representation
			EEnumLiteral enumLiteral = (EEnumLiteral) value;
			return enumLiteral.getLiteral();
		} else {
			// all other types of attributes don't need conversions
			return value;
		}
	}

	private static Object convertSinglePersistableObjectToEAttributeValue(final EAttribute eAttribute,
			final Object value) {
		if (value == null) {
			// null is always null, conversions make no sense here
			return null;
		}
		if (eAttribute.getEAttributeType() instanceof EEnum) {
			// for EEnums, we store the 'literal' representation, so we have to convert back now
			EEnum eEnum = (EEnum) eAttribute.getEAttributeType();
			if (value instanceof String == false) {
				throw new EObjectPersistenceException("Tried to deserialize value for EEnum-typed EAttribute '"
						+ eAttribute.getEContainingClass().getName() + "#" + eAttribute.getName()
						+ "', but the stored object (class: '" + value.getClass().getName()
						+ "' is not a literal string: '" + value + "'!");
			}
			EEnumLiteral enumLiteral = eEnum.getEEnumLiteralByLiteral((String) value);
			if (enumLiteral == null) {
				throw new EObjectPersistenceException("Tried to deserialize a value for EEnum-typed EAttribute '"
						+ eAttribute.getEContainingClass().getName() + "#" + eAttribute.getName()
						+ "', but the stored literal '" + value + "' has no representation in the EEnum '"
						+ eEnum.getName() + "'!");
			}
			return enumLiteral;
		}
		// anything else remains as-is
		return value;
	}

}
