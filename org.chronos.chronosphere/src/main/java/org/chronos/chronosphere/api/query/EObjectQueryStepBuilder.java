package org.chronos.chronosphere.api.query;

import org.eclipse.emf.ecore.*;

import java.util.Comparator;
import java.util.function.Predicate;

public interface EObjectQueryStepBuilder<S> extends QueryStepBuilder<S, EObject> {

    public EObjectQueryStepBuilder<S> orderBy(EAttribute eAttribute, Order order);

    @Override
    public EObjectQueryStepBuilder<S> limit(long limit);

    @Override
    public EObjectQueryStepBuilder<S> notNull();

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
    public EObjectQueryStepBuilder<S> has(String eStructuralFeatureName, Object value);

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
    public EObjectQueryStepBuilder<S> has(EStructuralFeature eStructuralFeature, Object value);

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
    public EObjectQueryStepBuilder<S> isInstanceOf(EClass eClass);

    /**
     * Filters all {@link EObject}s that are an instance of the given {@link EClass}.
     * <p>
     * <p>
     * This operation is <code>null</code>-safe. Any <code>null</code> values will be discarded and filtered out.
     *
     * @param eClass          The EClass to filter by. Must not be <code>null</code>.
     * @param allowSubclasses Whether or not subclasses are allowed. If <code>false</code>, any EObject that passes the filter must
     *                        be a direct instance of the given EClass. If <code>true</code>, EObjects that pass the filter can be a
     *                        direct or transitive instance of the given EClass.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public EObjectQueryStepBuilder<S> isInstanceOf(EClass eClass, boolean allowSubclasses);

    /**
     * Filters all {@link EObject}s that are a (direct or indirect) instance of an {@link EClass} with the given name.
     * <p>
     * <p>
     * This operation is <code>null</code>-safe. Any <code>null</code> values will be discarded and filtered out.
     *
     * @param eClassName The name of the EClass to search for. Must not be <code>null</code>.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public EObjectQueryStepBuilder<S> isInstanceOf(String eClassName);

    /**
     * Filters all {@link EObject}s that are a (direct or indirect) instance of an {@link EClass} with the given name.
     * <p>
     * <p>
     * This operation is <code>null</code>-safe. Any <code>null</code> values will be discarded and filtered out.
     *
     * @param eClassName      The name of the EClass to search for. Must not be <code>null</code>.
     * @param allowSubclasses Whether or not subclasses are allowed. If <code>false</code>, any EObject that passes the filter must
     *                        be a direct instance of the given EClass. If <code>true</code>, EObjects that pass the filter can be a
     *                        direct or transitive instance of the given EClass.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public EObjectQueryStepBuilder<S> isInstanceOf(String eClassName, boolean allowSubclasses);

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
    public UntypedQueryStepBuilder<S, Object> eGet(String eStructuralFeatureName);

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
    public EObjectQueryStepBuilder<S> eGet(EReference eReference);

    /**
     * Navigates along the <i>incoming</i> {@link EReference}s of the given type and forwards the {@link EObject}s on
     * the other end of the reference.
     *
     * @param eReference The EReference to follow backwards. Must not be <code>null</code>.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public EObjectQueryStepBuilder<S> eGetInverse(final EReference eReference);

    /**
     * Navigates along the <i>incoming</i> {@link EReference}s with the given name and forwards the {@link EObject}s on
     * the other end of the reference.
     *
     * @param referenceName The name of the reference to follow backwards. Must not be <code>null</code>.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public EObjectQueryStepBuilder<S> eGetInverse(final String referenceName);

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
    public UntypedQueryStepBuilder<S, Object> eGet(EAttribute eAttribute);

    /**
     * Navigates from an {@link EObject} to its {@link EObject#eContainer() eContainer}.
     *
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public EObjectQueryStepBuilder<S> eContainer();

    /**
     * Navigates from an {@link EObject} to its {@link EObject#eContents() eContents}.
     *
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public EObjectQueryStepBuilder<S> eContents();

    /**
     * Navigates from an {@link EObject} to its {@link EObject#eAllContents() eAllContents}.
     *
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public EObjectQueryStepBuilder<S> eAllContents();

    /**
     * Navigates from an {@link EObject} to all other {@link EObject}s that reference it.
     * <p>
     * <p>
     * This means walking backwards all {@link EReference} instances that have the current EObject as target. This
     * method will <b>include</b> the {@link EObject#eContainer() eContainer}.
     *
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public EObjectQueryStepBuilder<S> allReferencingEObjects();

    @Override
    public EObjectQueryStepBuilder<S> named(String name);

    @Override
    public EObjectQueryStepBuilder<S> filter(Predicate<EObject> predicate);

    @Override
    public EObjectQueryStepBuilder<S> orderBy(final Comparator<EObject> comparator);

    @Override
    public EObjectQueryStepBuilder<S> distinct();

    /**
     * Calculates the transitive closure of the current {@link EObject} that is created by repeatedly following the given (outgoing) {@link EReference}.
     * <p>
     * This method is safe to use in models that contain cyclic paths. Every EObject in the closure will only be returned <i>once</i>, even if multiple paths lead to it.
     * </p>
     * <p>
     * The EObject which is used to start the closure will <b>not</b> be part of the closure.
     * </p>
     * <p>
     * If an EObject is either of an {@link EClass} that does not own the given EReference, or the EObject itself does not have an instance of the EReference, the closure ends at this EObject.
     * </p>
     *
     * @param eReference The EReference to follow repeatedly. Must not be <code>null</code>.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public default EObjectQueryStepBuilder<S> closure(EReference eReference) {
        return this.closure(eReference, Direction.OUTGOING);
    }


    /**
     * Calculates the transitive closure of the current {@link EObject} that is created by repeatedly following the given (outgoing) {@link EReference}.
     * <p>
     * This method is safe to use in models that contain cyclic paths. Every EObject in the closure will only be returned <i>once</i>, even if multiple paths lead to it.
     * </p>
     * <p>
     * The EObject which is used to start the closure will <b>not</b> be part of the closure.
     * </p>
     * <p>
     * If an EObject is either of an {@link EClass} that does not own the given EReference, or the EObject itself does not have an instance of the EReference, the closure ends at this EObject.
     * </p>
     *
     * @param eReference The EReference to follow repeatedly. Must not be <code>null</code>.
     * @param direction  The direction to follow when calculating the closure. Must not be <code>null</code>.
     * @return The query builder, for method chaining. Never <code>null</code>.
     */
    public EObjectQueryStepBuilder<S> closure(EReference eReference, Direction direction);

}