package org.chronos.chronosphere.api.query;

import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.chronos.chronosphere.impl.query.EObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;

public class SubQuery {

	// =================================================================================================================
	// GENERIC QUERY METHODS
	// =================================================================================================================

	/**
	 * Applies the given filter predicate to all elements in the current result.
	 *
	 * @param predicate
	 *            The predicate to apply. Must not be <code>null</code>. All elements for which the predicate function
	 *            returns <code>false</code> will be filtered out and discarded.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public static <S> UntypedQueryStepBuilder<S, S> filter(final Predicate<S> predicate) {
		GraphTraversal<S, S> traversal = __.filter(traverser -> predicate.test(traverser.get()));
		return new ObjectQueryStepBuilderImpl<S, S>(null, traversal);
	}

	/**
	 * Uses the given function to map each element from the current result set to a new element.
	 *
	 * @param function
	 *            The mapping function to apply. Must not be <code>null</code>. Should be idempotent and side-effect
	 *            free.
	 *
	 * @return The query step builder, for method chaining. Never <code>null</code>.
	 */
	public static <S, E> UntypedQueryStepBuilder<S, E> map(final Function<S, E> function) {
		GraphTraversal<S, E> traversal = __.map(traverser -> function.apply(traverser.get()));
		return new ObjectQueryStepBuilderImpl<>(null, traversal);
	}

	/**
	 * Filters out and discards all <code>null</code> values from the current result set.
	 *
	 * <p>
	 * This is the same as:
	 *
	 * <pre>
	 * query.filter(element -> element != null)
	 * </pre>
	 *
	 * @return The query step builder, for method chaining. Never <code>null</code>.
	 *
	 * @see #filter(Predicate)
	 */
	public static <S> UntypedQueryStepBuilder<S, S> notNull() {
		return filter(element -> element != null);
	}

	// =================================================================================================================
	// TYPECAST METHODS
	// =================================================================================================================

	/**
	 * Converts the elements in the current result set into {@link EObject}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of EObject. If it is an EObject, it is cast
	 * down and forwarded. Otherwise, it is discarded, i.e. all non-EObjects will be filtered out. All <code>null</code>
	 * values will also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public static <S> EObjectQueryStepBuilder<S> asEObject() {
		UntypedQueryStepBuilder<S, S> start = startGeneric();
		return start.asEObject();
	}

	/**
	 * Converts the elements in the current result set into {@link Boolean}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Boolean. If it is a Boolean, it is cast
	 * down and forwarded. Otherwise, it is discarded, i.e. all non-Booleans will be filtered out. All <code>null</code>
	 * values will also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public static <S> QueryStepBuilder<S, Boolean> asBoolean() {
		UntypedQueryStepBuilder<S, S> start = startGeneric();
		return start.asBoolean();
	}

	/**
	 * Converts the elements in the current result set into {@link Byte}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Byte. If it is a Byte, it is cast down and
	 * forwarded. Otherwise, it is discarded, i.e. all non-Bytes will be filtered out. All <code>null</code> values will
	 * also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public static <S> NumericQueryStepBuilder<S, Byte> asByte() {
		UntypedQueryStepBuilder<S, S> start = startGeneric();
		return start.asByte();
	}

	/**
	 * Converts the elements in the current result set into {@link Short}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Short. If it is a Short, it is cast down
	 * and forwarded. Otherwise, it is discarded, i.e. all non-Shorts will be filtered out. All <code>null</code> values
	 * will also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public static <S> NumericQueryStepBuilder<S, Short> asShort() {
		UntypedQueryStepBuilder<S, S> start = startGeneric();
		return start.asShort();
	}

	/**
	 * Converts the elements in the current result set into {@link Character}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Character. If it is a Character, it is cast
	 * down and forwarded. Otherwise, it is discarded, i.e. all non-Characters will be filtered out. All
	 * <code>null</code> values will also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public static <S> QueryStepBuilder<S, Character> asCharacter() {
		UntypedQueryStepBuilder<S, S> start = startGeneric();
		return start.asCharacter();
	}

	/**
	 * Converts the elements in the current result set into {@link Integer}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Integer. If it is a Integer, it is cast
	 * down and forwarded. Otherwise, it is discarded, i.e. all non-Integers will be filtered out. All <code>null</code>
	 * values will also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public static <S> NumericQueryStepBuilder<S, Integer> asInteger() {
		UntypedQueryStepBuilder<S, S> start = startGeneric();
		return start.asInteger();
	}

	/**
	 * Converts the elements in the current result set into {@link Long}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Long. If it is a Long, it is cast down and
	 * forwarded. Otherwise, it is discarded, i.e. all non-Longs will be filtered out. All <code>null</code> values will
	 * also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public static <S> NumericQueryStepBuilder<S, Long> asLong() {
		UntypedQueryStepBuilder<S, S> start = startGeneric();
		return start.asLong();
	}

	/**
	 * Converts the elements in the current result set into {@link Float}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Float. If it is a Float, it is cast down
	 * and forwarded. Otherwise, it is discarded, i.e. all non-Floats will be filtered out. All <code>null</code> values
	 * will also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public static <S> NumericQueryStepBuilder<S, Float> asFloat() {
		UntypedQueryStepBuilder<S, S> start = startGeneric();
		return start.asFloat();
	}

	/**
	 * Converts the elements in the current result set into {@link Double}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Double. If it is a Double, it is cast down
	 * and forwarded. Otherwise, it is discarded, i.e. all non-Doubles will be filtered out. All <code>null</code>
	 * values will also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public static <S> NumericQueryStepBuilder<S, Double> asDouble() {
		UntypedQueryStepBuilder<S, S> start = startGeneric();
		return start.asDouble();
	}

	/**
	 * Assigns the given name to this {@link QueryStepBuilder}.
	 *
	 * <p>
	 * Note that only the <i>step</i> is named. When coming back to this step, the query result may be different than it
	 * was when it was first reached, depending on the traversal.
	 *
	 * @param stepName
	 *            The name of the step. Must not be <code>null</code>. Must be unique within the query.
	 *
	 * @return The named step, for method chaining. Never <code>null</code>.
	 */
	public static <S> QueryStepBuilder<S, S> named(final String stepName) {
		UntypedQueryStepBuilder<S, S> start = startGeneric();
		return start.named(stepName);
	}

	@SafeVarargs
	public static <S> QueryStepBuilder<S, Object> union(final QueryStepBuilder<S, ?>... subqueries) {
		UntypedQueryStepBuilder<S, S> start = startGeneric();
		return start.union(subqueries);
	}

	@SafeVarargs
	public static <S> QueryStepBuilder<S, S> and(final QueryStepBuilder<S, ?>... subqueries) {
		UntypedQueryStepBuilder<S, S> start = startGeneric();
		return start.and(subqueries);
	}

	@SafeVarargs
	public static <S> QueryStepBuilder<S, S> or(final QueryStepBuilder<S, ?>... subqueries) {
		UntypedQueryStepBuilder<S, S> start = startGeneric();
		return start.or(subqueries);
	}

	// =====================================================================================================================
	// EOBJECT METHODS
	// =====================================================================================================================

	/**
	 * Filters all EObjects in the result set that have the given feature set to the given value.
	 *
	 * <p>
	 * Any {@link EObject} that is an instance of an {@link EClass} which does not define any {@link EAttribute} or
	 * {@link EReference} with the given name will be discarded and filtered out.
	 *
	 * <p>
	 * For determining the feature, {@link EClass#getEStructuralFeature(String)} is used per {@link EObject}
	 * individually.
	 *
	 * <p>
	 * The value will be compared via {@link java.util.Objects#equals(Object, Object)}.
	 *
	 * @param eStructuralFeatureName
	 *            The name of the {@link EStructuralFeature} to filter by. Must not be <code>null</code>.
	 * @param value
	 *            The value to filter by. May be <code>null</code>.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public static EObjectQueryStepBuilder<EObject> has(final String eStructuralFeatureName, final Object value) {
		return startEObject().has(eStructuralFeatureName, value);
	}

	/**
	 * Filters all EObjects in the result set that have the given feature set to the given value.
	 *
	 * <p>
	 * Any {@link EObject} that belongs to an {@link EClass} that does not support the given feature will be discarded
	 * and filtered out.
	 *
	 * <p>
	 * The value will be compared via {@link java.util.Objects#equals(Object, Object)}.
	 *
	 * @param eStructuralFeature
	 *            The feature to filter by. Must not be <code>null</code>.
	 * @param value
	 *            The value to filter by. Must not be <code>null</code>.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public static EObjectQueryStepBuilder<EObject> has(final EStructuralFeature eStructuralFeature,
			final Object value) {
		return startEObject().has(eStructuralFeature, value);
	}

	/**
	 * Filters all {@link EObject}s that are an instance of the given {@link EClass}.
	 *
	 * <p>
	 * This method also considers polymorphism, i.e. instances of Sub-EClasses will pass this filter as well.
	 *
	 * <p>
	 * This operation is <code>null</code>-safe. Any <code>null</code> values will be discarded and filtered out.
	 *
	 * @param eClass
	 *            The EClass to filter by. Must not be <code>null</code>.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public static EObjectQueryStepBuilder<EObject> isInstanceOf(final EClass eClass) {
		return startEObject().isInstanceOf(eClass);
	}

	/**
	 * Filters all {@link EObject}s that are a (direct or indirect) instance of an {@link EClass} with the given name.
	 *
	 * <p>
	 * This operation is <code>null</code>-safe. Any <code>null</code> values will be discarded and filtered out.
	 *
	 * @param eClassName
	 *            The name of the EClass to search for. Must not be <code>null</code>.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public static EObjectQueryStepBuilder<EObject> isInstanceOf(final String eClassName) {
		return startEObject().isInstanceOf(eClassName);
	}

	/**
	 * Performs a {@link EObject#eGet(EStructuralFeature)} operation on all {@link EObject}s in the current result and
	 * forwards the result of this method.
	 *
	 * <p>
	 * If a given {@link EObject} does not have the {@link EStructuralFeature} set (i.e.
	 * {@link EObject#eIsSet(EStructuralFeature)} returns <code>false</code>), then no value will be passed onwards. In
	 * any other case, the returned value is passed onwards, even if that value is <code>null</code>.
	 *
	 * @param eStructuralFeatureName
	 *            The name of the {@link EStructuralFeature} to get the value(s) for. Must not be <code>null</code>.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public static UntypedQueryStepBuilder<EObject, Object> eGet(final String eStructuralFeatureName) {
		return startEObject().eGet(eStructuralFeatureName);
	}

	/**
	 * Performs a {@link EObject#eGet(EStructuralFeature)} operation on all {@link EObject}s in the current result and
	 * forwards the result of this method.
	 *
	 * <p>
	 * If a given {@link EObject} does not have the {@link EStructuralFeature} set (i.e.
	 * {@link EObject#eIsSet(EStructuralFeature)} returns <code>false</code>), then no value will be passed onwards. In
	 * any other case, the returned value is passed onwards, even if that value is <code>null</code>.
	 *
	 * @param eReference
	 *            The {@link EReference} to get the value(s) for. Must not be <code>null</code>.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public static EObjectQueryStepBuilder<EObject> eGet(final EReference eReference) {
		return startEObject().eGet(eReference);
	}

	/**
	 * Performs a {@link EObject#eGet(EStructuralFeature)} operation on all {@link EObject}s in the current result and
	 * forwards the result of this method.
	 *
	 * <p>
	 * If a given {@link EObject} does not have the {@link EStructuralFeature} set (i.e.
	 * {@link EObject#eIsSet(EStructuralFeature)} returns <code>false</code>), then no value will be passed onwards. In
	 * any other case, the returned value is passed onwards, even if that value is <code>null</code>.
	 *
	 * @param eAttribute
	 *            The {@link EAttribute} to get the value(s) for. Must not be <code>null</code>.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public static UntypedQueryStepBuilder<EObject, Object> eGet(final EAttribute eAttribute) {
		return startEObject().eGet(eAttribute);
	}

	/**
	 * Navigates from an {@link EObject} to its {@link EObject#eContainer() eContainer}.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public static EObjectQueryStepBuilder<EObject> eContainer() {
		return startEObject().eContainer();
	}

	/**
	 * Navigates from an {@link EObject} to its {@link EObject#eContents() eContents}.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public static EObjectQueryStepBuilder<EObject> eContents() {
		return startEObject().eContents();
	}

	/**
	 * Navigates from an {@link EObject} to its {@link EObject#eAllContents() eAllContents}.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public static EObjectQueryStepBuilder<EObject> eAllContents() {
		return startEObject().eAllContents();
	}

	/**
	 * Navigates from an {@link EObject} to all other {@link EObject}s that reference it.
	 *
	 * <p>
	 * This means walking backwards all {@link EReference} instances that have the current EObject as target. This
	 * method will <b>include</b> the {@link EObject#eContainer() eContainer}.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public static EObjectQueryStepBuilder<EObject> allReferencingEObjects() {
		return startEObject().allReferencingEObjects();
	}

	// =====================================================================================================================
	// INTERNAL METHODS
	// =====================================================================================================================

	private static <S> UntypedQueryStepBuilder<S, S> startGeneric() {
		GraphTraversal<S, S> traversal = __.identity();
		return new ObjectQueryStepBuilderImpl<>(null, traversal);
	}

	private static EObjectQueryStepBuilder<EObject> startEObject() {
		GraphTraversal<EObject, EObject> traversal = __.identity();
		return new EObjectQueryStepBuilderImpl<>(null, traversal);
	}
}
