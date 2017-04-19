package org.chronos.chronosphere.impl.query;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.api.query.EObjectQueryStepBuilder;
import org.chronos.chronosphere.api.query.Order;
import org.chronos.chronosphere.api.query.QueryStepBuilderInternal;
import org.chronos.chronosphere.api.query.UntypedQueryStepBuilder;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.chronos.chronosphere.internal.ogm.api.VertexKind;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;

public class EObjectQueryStepBuilderImpl<S> extends AbstractQueryStepBuilder<S, EObject>
		implements EObjectQueryStepBuilder<S> {

	public EObjectQueryStepBuilderImpl(final QueryStepBuilderInternal<S, ?> previous,
			final GraphTraversal<?, EObject> traversal) {
		super(previous, traversal);
	}

	@Override
	public EObjectQueryStepBuilder<S> orderBy(final Comparator<EObject> comparator) {
		checkNotNull(comparator, "Precondition violation - argument 'comparator' must not be NULL!");
		this.assertModificationsAllowed();
		return new EObjectQueryStepBuilderImpl<S>(this, this.getTraversal().order().by(comparator));
	}

	@Override
	public EObjectQueryStepBuilder<S> orderBy(final EAttribute eAttribute, final Order order) {
		checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		this.assertModificationsAllowed();
		return this.orderBy(new EObjectAttributeComparator(eAttribute, order));
	}

	@Override
	public EObjectQueryStepBuilder<S> distinct() {
		this.assertModificationsAllowed();
		return new EObjectQueryStepBuilderImpl<S>(this, this.getTraversal().dedup());
	}

	@Override
	public <T> UntypedQueryStepBuilder<S, T> map(final Function<EObject, T> function) {
		checkNotNull(function, "Precondition violation - argument 'function' must not be NULL!");
		this.assertModificationsAllowed();
		Function<Traverser<EObject>, T> traverserFunction = this.convertToTraverserMapFunction(function);
		return new ObjectQueryStepBuilderImpl<S, T>(this, this.getTraversal().map(traverserFunction));
	}

	@Override
	public Set<EObject> toSet() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		return this.getTraversal().toSet();
	}

	@Override
	public List<EObject> toList() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		return this.getTraversal().toList();
	}

	@Override
	public Stream<EObject> toStream() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		return this.getTraversal().toStream();
	}

	@Override
	public EObjectQueryStepBuilder<S> limit(final long limit) {
		checkArgument(limit >= 0, "Precondition violation - argument 'limit' must not be negative!");
		this.assertModificationsAllowed();
		return new EObjectQueryStepBuilderImpl<S>(this, this.getTraversal().limit(limit));
	}

	@Override
	public EObjectQueryStepBuilder<S> notNull() {
		this.assertModificationsAllowed();
		return this.filter(e -> e != null);
	}

	@Override
	public EObjectQueryStepBuilder<S> has(final String eStructuralFeatureName, final Object value) {
		checkNotNull(eStructuralFeatureName,
				"Precondition violation - argument 'eStructuralFeatureName' must not be NULL!");
		this.assertModificationsAllowed();
		return this.filter(eObject -> {
			if (eObject == null) {
				return false;
			}
			EClass eClass = eObject.eClass();
			EStructuralFeature feature = eClass.getEStructuralFeature(eStructuralFeatureName);
			if (feature == null) {
				return false;
			}

			// DON'T DO THIS! This will break EAttributes with enum types that have a default
			// value even when they are not set!
			// if (eObject.eIsSet(feature) == false) {
			// return false;
			// }

			return Objects.equal(eObject.eGet(feature), value);
		});
	}

	@Override
	public EObjectQueryStepBuilder<S> has(final EStructuralFeature eStructuralFeature, final Object value) {
		checkNotNull(eStructuralFeature, "Precondition violation - argument 'eStructuralFeature' must not be NULL!");
		this.assertModificationsAllowed();
		EStructuralFeature storedFeature = this.getTransaction().getEPackageRegistry()
				.getRegisteredEStructuralFeature(eStructuralFeature);
		return this.filter(eObject -> {
			if (eObject == null) {
				return false;
			}

			if (eObject.eClass().getEAllStructuralFeatures().contains(storedFeature) == false) {
				// EClass does not support this feature
				return false;
			}

			// DON'T DO THIS!! This creates unneccessary trouble, e.g. when the value is an EEnum. They have
			// a default value set up which will be returned on eGet() even when the feature was never set before.

			// if (eObject.eIsSet(storedFeature) == false) {
			// // EObject does not have this feature assigned
			// return false;
			// }

			Object v = value;
			Object storedValue = eObject.eGet(storedFeature);
			boolean areEqual = Objects.equal(storedValue, v);
			return areEqual;
		});
	}

	@Override
	public EObjectQueryStepBuilder<S> isInstanceOf(final EClass eClass) {
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		return this.isInstanceOf(eClass, true);
	}

	@Override
	public EObjectQueryStepBuilder<S> isInstanceOf(final EClass eClass, final boolean allowSubclasses) {
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		this.assertModificationsAllowed();
		if (allowSubclasses == false) {
			return this.filter(eObject -> {
				if (eObject == null) {
					return false;
				}
				return Objects.equal(eObject.eClass(), eClass);
			});
		} else {
			return this.filter(eObject -> {
				if (eObject == null) {
					return false;
				}
				return eClass.isSuperTypeOf(eObject.eClass());
			});
		}
	}

	@Override
	public EObjectQueryStepBuilder<S> isInstanceOf(final String eClassName) {
		checkNotNull(eClassName, "Precondition violation - argument 'eClassName' must not be NULL!");
		return this.isInstanceOf(eClassName, true);
	}

	@Override
	public EObjectQueryStepBuilder<S> isInstanceOf(final String eClassName, final boolean allowSubclasses) {
		checkNotNull(eClassName, "Precondition violation - argument 'eClassName' must not be NULL!");
		this.assertModificationsAllowed();
		// try to find the EClass by qualified name
		EClass eClass = this.getTransaction().getEClassByQualifiedName(eClassName);
		if (eClass == null) {
			// try again, by simple name
			eClass = this.getTransaction().getEClassBySimpleName(eClassName);
		}
		if (eClass == null) {
			throw new IllegalArgumentException("Could not find EClass with name '" + eClassName + "'!");
		}
		return this.isInstanceOf(eClass, allowSubclasses);
	}

	@Override
	@SuppressWarnings("unchecked")
	public UntypedQueryStepBuilder<S, Object> eGet(final String eStructuralFeatureName) {
		checkNotNull(eStructuralFeatureName,
				"Precondition violation - argument 'eStructuralFeatureName' must not be NULL!");
		this.assertModificationsAllowed();
		GraphTraversal<?, Object> newTraversal = this.getTraversal().flatMap(traverser -> {
			if (traverser == null || traverser.get() == null) {
				// skip NULL objects
				return Collections.emptyIterator();
			}
			EObject eObject = traverser.get();
			EStructuralFeature feature = eObject.eClass().getEStructuralFeature(eStructuralFeatureName);
			if (feature == null) {
				return Collections.emptyIterator();
			}
			if (eObject.eIsSet(feature) == false) {
				return Collections.emptyIterator();
			}
			Iterator<Object> iterator = null;
			if (feature.isMany() == false) {
				// multiplicity-one feature
				iterator = Collections.singleton(eObject.eGet(feature)).iterator();
			} else {
				// multiplicity-many feature
				iterator = ((List<Object>) eObject.eGet(feature)).iterator();
			}
			return iterator;
		});
		return new ObjectQueryStepBuilderImpl<S, Object>(this, newTraversal);
	}

	@Override
	@SuppressWarnings("unchecked")
	public EObjectQueryStepBuilder<S> eGet(final EReference eReference) {
		checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
		this.assertModificationsAllowed();
		GraphTraversal<?, EObject> newTraversal = this.getTraversal().filter(t -> t.get() != null)
				.flatMap(traverser -> {
					if (traverser == null || traverser.get() == null) {
						// skip NULL objects
						return Collections.emptyIterator();
					}
					EObject eObject = traverser.get();
					if (eObject.eClass().getEAllReferences().contains(eReference) == false) {
						// EClass does not support this feature
						return Collections.emptyIterator();
					}
					if (eObject.eIsSet(eReference) == false) {
						// EReference is not set on the EObject
						return Collections.emptyIterator();
					}
					Iterator<EObject> iterator = null;
					if (eReference.isMany() == false) {
						// multiplicity-one
						iterator = Collections.singleton((EObject) eObject.eGet(eReference)).iterator();
					} else {
						// multiplicity-many
						iterator = ((List<EObject>) eObject.eGet(eReference)).iterator();
					}
					return iterator;
				});
		return new EObjectQueryStepBuilderImpl<S>(this, newTraversal);
	}

	@Override
	public EObjectQueryStepBuilder<S> eGetInverse(final EReference eReference) {
		this.assertModificationsAllowed();
		ChronoEPackageRegistry registry = this.getTransaction().getEPackageRegistry();
		String edgeLabel = ChronoSphereGraphFormat.createReferenceEdgeLabel(registry, eReference);
		GraphTraversal<?, EObject> newTraversal = this.getTraversal().flatMap(traverser -> {
			if (traverser == null || traverser.get() == null) {
				// skip NULL objects
				return Collections.emptyIterator();
			}
			EObject eObject = traverser.get();
			ChronoEObject chronoEObject = (ChronoEObject) eObject;
			GraphTraversal<?, Vertex> innerTraversal = this.getTransaction().getGraph().traversal()
					.V(chronoEObject.getId()).in(edgeLabel)
					.has(ChronoSphereGraphFormat.V_PROP__KIND, VertexKind.EOBJECT.toString());
			return Iterators.transform(innerTraversal, this::mapVertexToEObject);
		});
		return new EObjectQueryStepBuilderImpl<S>(this, newTraversal);
	}

	@Override
	@SuppressWarnings("unchecked")
	public EObjectQueryStepBuilder<S> eGetInverse(final String referenceName) {
		this.assertModificationsAllowed();
		ChronoEPackageRegistry registry = this.getTransaction().getEPackageRegistry();
		SetMultimap<EClass, EReference> eClassToIncomingReferences = EMFUtils
				.eClassToIncomingEReferences(registry.getEPackages());
		GraphTraversal<?, EObject> newTraversal = this.getTraversal().flatMap(traverser -> {
			if (traverser == null || traverser.get() == null) {
				// skip NULL objects
				return Collections.emptyIterator();
			}
			ChronoEObject eObject = (ChronoEObject) traverser.get();
			EClass eClass = eObject.eClass();
			Set<EReference> references = eClassToIncomingReferences.get(eClass);
			List<Iterator<EObject>> iterators = Lists.newArrayList();
			for (EReference eReference : references) {
				if (eReference.getName().equals(referenceName) == false) {
					continue;
				}
				String edgeLabel = ChronoSphereGraphFormat.createReferenceEdgeLabel(registry, eReference);
				GraphTraversal<?, Vertex> innerTraversal = this.getTransaction().getGraph().traversal()
						.V(eObject.getId()).in(edgeLabel)
						.has(ChronoSphereGraphFormat.V_PROP__KIND, VertexKind.EOBJECT.toString());
				iterators.add(Iterators.transform(innerTraversal, this::mapVertexToEObject));
			}
			Iterator<EObject> concat = Iterators.concat(iterators.toArray(new Iterator[iterators.size()]));
			return concat;
		});
		return new EObjectQueryStepBuilderImpl<S>(this, newTraversal);
	}

	@Override
	@SuppressWarnings("unchecked")
	public UntypedQueryStepBuilder<S, Object> eGet(final EAttribute eAttribute) {
		checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
		this.assertModificationsAllowed();
		GraphTraversal<?, Object> newTraversal = this.getTraversal().flatMap(traverser -> {
			if (traverser == null || traverser.get() == null) {
				// skip NULL objects
				return Collections.emptyIterator();
			}
			EObject eObject = traverser.get();
			if (eObject.eClass().getEAllAttributes().contains(eAttribute) == false) {
				// EClass does not support this feature
				return Collections.emptyIterator();
			}
			if (eObject.eIsSet(eAttribute) == false) {
				// EAttribute is not set on the EObject
				return Collections.emptyIterator();
			}
			if (eAttribute.isMany() == false) {
				// multiplicity-one
				return Collections.singleton(eObject.eGet(eAttribute)).iterator();
			} else {
				// multiplicity-many
				return ((List<Object>) eObject.eGet(eAttribute)).iterator();
			}
		});
		return new ObjectQueryStepBuilderImpl<S, Object>(this, newTraversal);
	}

	@Override
	public EObjectQueryStepBuilder<S> eContainer() {
		this.assertModificationsAllowed();
		GraphTraversal<?, EObject> newTraversal = this.getTraversal().flatMap(traverser -> {
			if (traverser == null || traverser.get() == null) {
				// skip NULL objects
				return Collections.emptyIterator();
			}
			EObject eObject = traverser.get();
			EObject eContainer = eObject.eContainer();
			if (eContainer == null) {
				return Collections.emptyIterator();
			}
			return Collections.singleton(eContainer).iterator();
		});
		return new EObjectQueryStepBuilderImpl<S>(this, newTraversal);
	}

	@Override
	public EObjectQueryStepBuilder<S> eContents() {
		this.assertModificationsAllowed();
		GraphTraversal<?, EObject> newTraversal = this.getTraversal().flatMap(traverser -> {
			if (traverser == null || traverser.get() == null) {
				// skip NULL objects
				return Collections.emptyIterator();
			}
			EObject eObject = traverser.get();
			Iterator<EObject> eContents = eObject.eContents().iterator();
			return eContents;
		});
		return new EObjectQueryStepBuilderImpl<S>(this, newTraversal);
	}

	@Override
	public EObjectQueryStepBuilder<S> eAllContents() {
		this.assertModificationsAllowed();
		GraphTraversal<?, EObject> newTraversal = this.getTraversal().flatMap(traverser -> {
			if (traverser == null || traverser.get() == null) {
				// skip NULL objects
				return Collections.emptyIterator();
			}
			EObject eObject = traverser.get();
			Iterator<EObject> eAllContents = eObject.eAllContents();
			if (eAllContents == null) {
				return Collections.emptyIterator();
			}
			return eAllContents;
		});
		return new EObjectQueryStepBuilderImpl<S>(this, newTraversal);
	}

	@Override
	public EObjectQueryStepBuilder<S> allReferencingEObjects() {
		this.assertModificationsAllowed();
		GraphTraversal<?, EObject> newTraversal = this.getTraversal().flatMap(traverser -> {
			if (traverser == null || traverser.get() == null) {
				// skip NULL objects
				return Collections.emptyIterator();
			}
			EObject eObject = traverser.get();
			ChronoEObject chronoEObject = (ChronoEObject) eObject;
			GraphTraversal<?, Vertex> innerTraversal = this.getTransaction().getGraph().traversal()
					.V(chronoEObject.getId()).in()
					.has(ChronoSphereGraphFormat.V_PROP__KIND, VertexKind.EOBJECT.toString());
			return Iterators.transform(innerTraversal, this::mapVertexToEObject);
		});
		return new EObjectQueryStepBuilderImpl<S>(this, newTraversal);
	}

	@Override
	public EObjectQueryStepBuilder<S> named(final String name) {
		this.assertModificationsAllowed();
		return new EObjectQueryStepBuilderImpl<S>(this, this.getTraversal().as(name));
	}

	@Override
	public EObjectQueryStepBuilder<S> filter(final Predicate<EObject> predicate) {
		checkNotNull(predicate, "Precondition violation - argument 'predicate' must not be NULL!");
		this.assertModificationsAllowed();
		Predicate<Traverser<EObject>> traverserPredicate = this.convertToEObjectTraverserPredicate(predicate);
		return new EObjectQueryStepBuilderImpl<S>(this, this.getTraversal().filter(traverserPredicate));
	}

	// =====================================================================================================================
	// HELPER METHODS
	// =====================================================================================================================

	private <T> Function<Traverser<EObject>, T> convertToTraverserMapFunction(final Function<EObject, T> function) {
		checkNotNull(function, "Precondition violation - argument 'function' must not be NULL!");
		return (traverser) -> {
			EObject eObject = traverser.get();
			return function.apply(eObject);
		};
	}

	private Predicate<Traverser<EObject>> convertToEObjectTraverserPredicate(final Predicate<EObject> predicate) {
		checkNotNull(predicate, "Precondition violation - argument 'predicate' must not be NULL!");
		return (traverser) -> {
			EObject eObject = traverser.get();
			return predicate.test(eObject);
		};
	}

	private EObject mapVertexToEObject(final Vertex vertex) {
		if (vertex == null) {
			return null;
		}
		EObject eObject = this.getTransaction().getEObjectById((String) vertex.id());
		return eObject;
	}

}
