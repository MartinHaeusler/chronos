package org.chronos.chronosphere.impl.query;

import org.chronos.chronosphere.api.query.EObjectQueryStepBuilder;
import org.chronos.chronosphere.api.query.NumericQueryStepBuilder;
import org.chronos.chronosphere.api.query.QueryStepBuilder;
import org.chronos.chronosphere.api.query.UntypedQueryStepBuilder;
import org.chronos.chronosphere.impl.query.steps.eobject.EObjectQueryAsEObjectStepBuilder;
import org.chronos.chronosphere.impl.query.steps.numeric.*;
import org.chronos.chronosphere.impl.query.steps.object.ObjectQueryAsBooleanStepBuilder;
import org.chronos.chronosphere.impl.query.steps.object.ObjectQueryAsCharacterStepBuilder;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;

import java.util.function.Predicate;

public abstract class ObjectQueryStepBuilderImpl<S, I, E> extends AbstractQueryStepBuilder<S, I, E> implements UntypedQueryStepBuilder<S, E> {

    public ObjectQueryStepBuilderImpl(final TraversalChainElement previous) {
        super(previous);
    }

    // =====================================================================================================================
    // PUBLIC API
    // =====================================================================================================================

    @Override
    public EObjectQueryStepBuilder<S> asEObject() {
        return new EObjectQueryAsEObjectStepBuilder<>(this);
    }

    @Override
    public QueryStepBuilder<S, Boolean> asBoolean() {
        return new ObjectQueryAsBooleanStepBuilder<>(this);
    }

    @Override
    public NumericQueryStepBuilder<S, Byte> asByte() {
        return new NumericQueryAsByteStepBuilder<>(this);
    }

    @Override
    public NumericQueryStepBuilder<S, Short> asShort() {
        return new NumericQueryAsShortStepBuilder<>(this);
    }

    @Override
    public QueryStepBuilder<S, Character> asCharacter() {
        return new ObjectQueryAsCharacterStepBuilder<>(this);
    }

    @Override
    public NumericQueryStepBuilder<S, Integer> asInteger() {
        return new NumericQueryAsIntegerStepBuilder<>(this);
    }

    @Override
    public NumericQueryStepBuilder<S, Long> asLong() {
        return new NumericQueryAsLongStepBuilder<>(this);
    }

    @Override
    public NumericQueryStepBuilder<S, Float> asFloat() {
        return new NumericQueryAsFloatStepBuilder<>(this);
    }

    @Override
    public NumericQueryStepBuilder<S, Double> asDouble() {
        return new NumericQueryAsDoubleStepBuilder<>(this);
    }

    @Override
    public UntypedQueryStepBuilder<S, E> filter(final Predicate<E> predicate) {
        return (UntypedQueryStepBuilder<S, E>) super.filter(predicate);
    }
}
