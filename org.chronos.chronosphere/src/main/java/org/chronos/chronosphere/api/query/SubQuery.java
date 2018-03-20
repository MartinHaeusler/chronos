package org.chronos.chronosphere.api.query;

import org.chronos.chronosphere.impl.query.steps.eobject.*;
import org.chronos.chronosphere.impl.query.steps.numeric.*;
import org.chronos.chronosphere.impl.query.steps.object.*;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.impl.query.traversal.TraversalSource;
import org.eclipse.emf.ecore.*;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

public class SubQuery {

    // =================================================================================================================
    // GENERIC QUERY METHODS
    // =================================================================================================================

    /**
     * Applies the given filter predicate to all elements in the current result.
     *
     * @param predicate The predicate to apply. Must not be <code>null</code>. All elements for which the predicate function
     *                  returns <code>false</code> will be filtered out and discarded.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static <S> UntypedQueryStepBuilder<S, S> filter(final Predicate<S> predicate) {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new ObjectQueryFilterStepBuilder<>(source, predicate);
    }

    /**
     * Uses the given function to map each element from the current result set to a new element.
     *
     * @param function The mapping function to apply. Must not be <code>null</code>. Should be idempotent and side-effect
     *                 free.
     * @return The query step builder, for method chaining. Never <code>null</code>.
     */
    public static <S, E> UntypedQueryStepBuilder<S, E> map(final Function<S, E> function) {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new ObjectQueryMapStepBuilder<>(source, function);
    }

    /**
     * Filters out and discards all <code>null</code> values from the current result set.
     * <p>
     * <p>
     * This is the same as:
     * <p>
     * <pre>
     * query.filter(element -> element != null)
     * </pre>
     *
     * @return The query step builder, for method chaining. Never <code>null</code>.
     * @see #filter(Predicate)
     */
    public static <S> UntypedQueryStepBuilder<S, S> notNull() {
        return filter(Objects::nonNull);
    }

    // =================================================================================================================
    // TYPECAST METHODS
    // =================================================================================================================

    /**
     * Converts the elements in the current result set into {@link EObject}s.
     * <p>
     * <p>
     * This method first checks if the element in question is an instance of EObject. If it is an EObject, it is cast
     * down and forwarded. Otherwise, it is discarded, i.e. all non-EObjects will be filtered out. All <code>null</code>
     * values will also be filtered out.
     *
     * @return The step builder, for method chaining. Never <code>null</code>.
     */
    public static <S> EObjectQueryStepBuilder<S> asEObject() {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new EObjectQueryAsEObjectStepBuilder<>(source);
    }

    /**
     * Converts the elements in the current result set into {@link Boolean}s.
     * <p>
     * <p>
     * This method first checks if the element in question is an instance of Boolean. If it is a Boolean, it is cast
     * down and forwarded. Otherwise, it is discarded, i.e. all non-Booleans will be filtered out. All <code>null</code>
     * values will also be filtered out.
     *
     * @return The step builder, for method chaining. Never <code>null</code>.
     */
    public static <S> QueryStepBuilder<S, Boolean> asBoolean() {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new ObjectQueryAsBooleanStepBuilder<>(source);
    }

    /**
     * Converts the elements in the current result set into {@link Byte}s.
     * <p>
     * <p>
     * This method first checks if the element in question is an instance of Byte. If it is a Byte, it is cast down and
     * forwarded. Otherwise, it is discarded, i.e. all non-Bytes will be filtered out. All <code>null</code> values will
     * also be filtered out.
     *
     * @return The step builder, for method chaining. Never <code>null</code>.
     */
    public static <S> NumericQueryStepBuilder<S, Byte> asByte() {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new NumericQueryAsByteStepBuilder<>(source);
    }

    /**
     * Converts the elements in the current result set into {@link Short}s.
     * <p>
     * <p>
     * This method first checks if the element in question is an instance of Short. If it is a Short, it is cast down
     * and forwarded. Otherwise, it is discarded, i.e. all non-Shorts will be filtered out. All <code>null</code> values
     * will also be filtered out.
     *
     * @return The step builder, for method chaining. Never <code>null</code>.
     */
    public static <S> NumericQueryStepBuilder<S, Short> asShort() {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new NumericQueryAsShortStepBuilder<>(source);
    }

    /**
     * Converts the elements in the current result set into {@link Character}s.
     * <p>
     * <p>
     * This method first checks if the element in question is an instance of Character. If it is a Character, it is cast
     * down and forwarded. Otherwise, it is discarded, i.e. all non-Characters will be filtered out. All
     * <code>null</code> values will also be filtered out.
     *
     * @return The step builder, for method chaining. Never <code>null</code>.
     */
    public static <S> QueryStepBuilder<S, Character> asCharacter() {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new ObjectQueryAsCharacterStepBuilder<>(source);
    }

    /**
     * Converts the elements in the current result set into {@link Integer}s.
     * <p>
     * <p>
     * This method first checks if the element in question is an instance of Integer. If it is a Integer, it is cast
     * down and forwarded. Otherwise, it is discarded, i.e. all non-Integers will be filtered out. All <code>null</code>
     * values will also be filtered out.
     *
     * @return The step builder, for method chaining. Never <code>null</code>.
     */
    public static <S> NumericQueryStepBuilder<S, Integer> asInteger() {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new NumericQueryAsIntegerStepBuilder<>(source);
    }

    /**
     * Converts the elements in the current result set into {@link Long}s.
     * <p>
     * <p>
     * This method first checks if the element in question is an instance of Long. If it is a Long, it is cast down and
     * forwarded. Otherwise, it is discarded, i.e. all non-Longs will be filtered out. All <code>null</code> values will
     * also be filtered out.
     *
     * @return The step builder, for method chaining. Never <code>null</code>.
     */
    public static <S> NumericQueryStepBuilder<S, Long> asLong() {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new NumericQueryAsLongStepBuilder<>(source);
    }

    /**
     * Converts the elements in the current result set into {@link Float}s.
     * <p>
     * <p>
     * This method first checks if the element in question is an instance of Float. If it is a Float, it is cast down
     * and forwarded. Otherwise, it is discarded, i.e. all non-Floats will be filtered out. All <code>null</code> values
     * will also be filtered out.
     *
     * @return The step builder, for method chaining. Never <code>null</code>.
     */
    public static <S> NumericQueryStepBuilder<S, Float> asFloat() {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new NumericQueryAsFloatStepBuilder<>(source);
    }

    /**
     * Converts the elements in the current result set into {@link Double}s.
     * <p>
     * <p>
     * This method first checks if the element in question is an instance of Double. If it is a Double, it is cast down
     * and forwarded. Otherwise, it is discarded, i.e. all non-Doubles will be filtered out. All <code>null</code>
     * values will also be filtered out.
     *
     * @return The step builder, for method chaining. Never <code>null</code>.
     */
    public static <S> NumericQueryStepBuilder<S, Double> asDouble() {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new NumericQueryAsDoubleStepBuilder<>(source);
    }

    /**
     * Assigns the given name to this {@link QueryStepBuilder}.
     * <p>
     * <p>
     * Note that only the <i>step</i> is named. When coming back to this step, the query result may be different than it
     * was when it was first reached, depending on the traversal.
     *
     * @param stepName The name of the step. Must not be <code>null</code>. Must be unique within the query.
     * @return The named step, for method chaining. Never <code>null</code>.
     */
    public static <S> QueryStepBuilder<S, S> named(final String stepName) {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new ObjectQueryNamedStepBuilder<>(source, stepName);
    }

    @SafeVarargs
    public static <S> QueryStepBuilder<S, Object> union(final QueryStepBuilder<S, ?>... subqueries) {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new ObjectQueryUnionStepBuilder<>(source, subqueries);
    }

    @SafeVarargs
    public static <S> QueryStepBuilder<S, S> and(final QueryStepBuilder<S, ?>... subqueries) {
        TraversalChainElement source = TraversalSource.createAnonymousSource();
        return new ObjectQueryAndStepBuilder<>(source, subqueries);
    }

    @SafeVarargs
    public static <S> QueryStepBuilder<S, S> or(final QueryStepBuilder<S, ?>... subqueries) {
        TraversalSource source = TraversalSource.createAnonymousSource();
        return new ObjectQueryOrStepBuilder<>(source, subqueries);
    }

    // =====================================================================================================================
    // EOBJECT METHODS
    // =====================================================================================================================

    /**
     * Filters all EObjects in the result set that have the given feature set to the given value.
     * <p>
     * <p>
     * Any {@link EObject} that is an instance of an {@link EClass} which does not define any {@link EAttribute} or
     * {@link EReference} with the given name will be discarded and filtered out.
     * <p>
     * <p>
     * For determining the feature, {@link EClass#getEStructuralFeature(String)} is used per {@link EObject}
     * individually.
     * <p>
     * <p>
     * The value will be compared via {@link java.util.Objects#equals(Object, Object)}.
     *
     * @param eStructuralFeatureName The name of the {@link EStructuralFeature} to filter by. Must not be <code>null</code>.
     * @param value                  The value to filter by. May be <code>null</code>.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static EObjectQueryStepBuilder<EObject> has(final String eStructuralFeatureName, final Object value) {
        TraversalSource<EObject, EObject> source = TraversalSource.createAnonymousSource();
        // we have to apply this on the EObject API, sadly
        ObjectQueryEObjectReifyStepBuilder<Object> reified = new ObjectQueryEObjectReifyStepBuilder<>(source);
        ObjectQueryFilterStepBuilder<Object, EObject> filtered = new ObjectQueryFilterStepBuilder<>(reified, (eObject -> {
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

            return com.google.common.base.Objects.equal(eObject.eGet(feature), value);
        }));
        return new EObjectQueryAsEObjectStepBuilder<>(filtered);
    }

    /**
     * Filters all EObjects in the result set that have the given feature set to the given value.
     * <p>
     * <p>
     * Any {@link EObject} that belongs to an {@link EClass} that does not support the given feature will be discarded
     * and filtered out.
     * <p>
     * <p>
     * The value will be compared via {@link java.util.Objects#equals(Object, Object)}.
     *
     * @param eStructuralFeature The feature to filter by. Must not be <code>null</code>.
     * @param value              The value to filter by. Must not be <code>null</code>.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static EObjectQueryStepBuilder<EObject> has(final EStructuralFeature eStructuralFeature,
                                                       final Object value) {
        TraversalSource<EObject, EObject> source = TraversalSource.createAnonymousSource();
        return new EObjectQueryHasFeatureValueStepBuilder<>(source, eStructuralFeature, value);
    }

    /**
     * Filters all {@link EObject}s that are an instance of the given {@link EClass}.
     * <p>
     * <p>
     * This method also considers polymorphism, i.e. instances of Sub-EClasses will pass this filter as well.
     * <p>
     * <p>
     * This operation is <code>null</code>-safe. Any <code>null</code> values will be discarded and filtered out.
     *
     * @param eClass The EClass to filter by. Must not be <code>null</code>.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static EObjectQueryStepBuilder<EObject> isInstanceOf(final EClass eClass) {
        return isInstanceOf(eClass, true);
    }


    /**
     * Filters all {@link EObject}s that are an instance of the given {@link EClass}.
     * <p>
     * This operation is <code>null</code>-safe. Any <code>null</code> values will be discarded and filtered out.
     *
     * @param eClass          The EClass to filter by. Must not be <code>null</code>.
     * @param allowSubclasses Whether or not to allow subclasses of the given class.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static EObjectQueryStepBuilder<EObject> isInstanceOf(final EClass eClass, boolean allowSubclasses) {
        TraversalSource<EObject, EObject> source = TraversalSource.createAnonymousSource();
        return new EObjectQueryInstanceOfEClassStepBuilder<>(source, eClass, allowSubclasses);
    }

    /**
     * Filters all {@link EObject}s that are a (direct or indirect) instance of an {@link EClass} with the given name.
     * <p>
     * This method also considers polymorphism, i.e. instances of Sub-EClasses will pass this filter as well.
     * <p>
     * This operation is <code>null</code>-safe. Any <code>null</code> values will be discarded and filtered out.
     *
     * @param eClassName The name of the EClass to search for. Must not be <code>null</code>.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static EObjectQueryStepBuilder<EObject> isInstanceOf(final String eClassName) {
        return isInstanceOf(eClassName, true);
    }

    /**
     * Filters all {@link EObject}s that are a (direct or indirect) instance of an {@link EClass} with the given name.
     * <p>
     * This operation is <code>null</code>-safe. Any <code>null</code> values will be discarded and filtered out.
     *
     * @param eClassName      The name of the EClass to search for. Must not be <code>null</code>.
     * @param allowSubclasses Whether or not to allow subclasses of the given class.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static EObjectQueryStepBuilder<EObject> isInstanceOf(final String eClassName, boolean allowSubclasses) {
        TraversalSource<EObject, EObject> source = TraversalSource.createAnonymousSource();
        return new EObjectQueryInstanceOfEClassNameStepBuilder<>(source, eClassName, allowSubclasses);
    }

    /**
     * Performs a {@link EObject#eGet(EStructuralFeature)} operation on all {@link EObject}s in the current result and
     * forwards the result of this method.
     * <p>
     * <p>
     * If a given {@link EObject} does not have the {@link EStructuralFeature} set (i.e.
     * {@link EObject#eIsSet(EStructuralFeature)} returns <code>false</code>), then no value will be passed onwards. In
     * any other case, the returned value is passed onwards, even if that value is <code>null</code>.
     *
     * @param eStructuralFeatureName The name of the {@link EStructuralFeature} to get the value(s) for. Must not be <code>null</code>.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static UntypedQueryStepBuilder<EObject, Object> eGet(final String eStructuralFeatureName) {
        TraversalSource<EObject, EObject> source = TraversalSource.createAnonymousSource();
        return new ObjectQueryEGetByNameStepBuilder<>(source, eStructuralFeatureName);
    }

    /**
     * Performs a {@link EObject#eGet(EStructuralFeature)} operation on all {@link EObject}s in the current result and
     * forwards the result of this method.
     * <p>
     * <p>
     * If a given {@link EObject} does not have the {@link EStructuralFeature} set (i.e.
     * {@link EObject#eIsSet(EStructuralFeature)} returns <code>false</code>), then no value will be passed onwards. In
     * any other case, the returned value is passed onwards, even if that value is <code>null</code>.
     *
     * @param eReference The {@link EReference} to get the value(s) for. Must not be <code>null</code>.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static EObjectQueryStepBuilder<EObject> eGet(final EReference eReference) {
        TraversalSource<EObject, EObject> source = TraversalSource.createAnonymousSource();
        return new EObjectQueryEGetReferenceStepBuilder<>(source, eReference);
    }

    /**
     * Performs a {@link EObject#eGet(EStructuralFeature)} operation on all {@link EObject}s in the current result and
     * forwards the result of this method.
     * <p>
     * <p>
     * If a given {@link EObject} does not have the {@link EStructuralFeature} set (i.e.
     * {@link EObject#eIsSet(EStructuralFeature)} returns <code>false</code>), then no value will be passed onwards. In
     * any other case, the returned value is passed onwards, even if that value is <code>null</code>.
     *
     * @param eAttribute The {@link EAttribute} to get the value(s) for. Must not be <code>null</code>.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static UntypedQueryStepBuilder<EObject, Object> eGet(final EAttribute eAttribute) {
        TraversalSource<EObject, EObject> source = TraversalSource.createAnonymousSource();
        return new ObjectQueryEGetAttributeStepBuilder<>(source, eAttribute);
    }

    /**
     * Navigates from an {@link EObject} to its {@link EObject#eContainer() eContainer}.
     *
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static EObjectQueryStepBuilder<EObject> eContainer() {
        TraversalSource<EObject, EObject> source = TraversalSource.createAnonymousSource();
        return new EObjectQueryEContainerStepBuilder<>(source);
    }

    /**
     * Navigates from an {@link EObject} to its {@link EObject#eContents() eContents}.
     *
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static EObjectQueryStepBuilder<EObject> eContents() {
        TraversalSource<EObject, EObject> source = TraversalSource.createAnonymousSource();
        return new EObjectQueryEContentsStepBuilder<>(source);
    }

    /**
     * Navigates from an {@link EObject} to its {@link EObject#eAllContents() eAllContents}.
     *
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static EObjectQueryStepBuilder<EObject> eAllContents() {
        TraversalSource<EObject, EObject> source = TraversalSource.createAnonymousSource();
        return new EObjectQueryEAllContentsStepBuilder<>(source);
    }

    /**
     * Navigates from an {@link EObject} to all other {@link EObject}s that reference it.
     * <p>
     * <p>
     * This means walking backwards all {@link EReference} instances that have the current EObject as target. This
     * method will <b>include</b> the {@link EObject#eContainer() eContainer}.
     *
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public static EObjectQueryStepBuilder<EObject> allReferencingEObjects() {
        TraversalSource<EObject, EObject> source = TraversalSource.createAnonymousSource();
        return new EObjectQueryAllReferencingEObjectsQueryStep<>(source);
    }

}
