package org.chronos.chronosphere.api.query;

import org.eclipse.emf.ecore.EObject;

public interface UntypedQueryStepBuilder<S, E> extends QueryStepBuilder<S, E>, NumericCastableQueryStepBuilder<S, E> {

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
    public EObjectQueryStepBuilder<S> asEObject();

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
    public QueryStepBuilder<S, Boolean> asBoolean();

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
    public QueryStepBuilder<S, Character> asCharacter();

}
