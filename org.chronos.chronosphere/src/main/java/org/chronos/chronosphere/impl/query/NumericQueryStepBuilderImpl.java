package org.chronos.chronosphere.impl.query;

import org.chronos.chronosphere.api.query.NumericQueryStepBuilder;
import org.chronos.chronosphere.impl.query.steps.numeric.*;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;

import java.util.Iterator;
import java.util.Optional;

public abstract class NumericQueryStepBuilderImpl<S, I, E extends Number> extends AbstractQueryStepBuilder<S, I, E>
    implements NumericQueryStepBuilder<S, E> {


    public NumericQueryStepBuilderImpl(final TraversalChainElement previous) {
        super(previous);
    }

    @Override
    public Optional<Double> sumAsDouble() {
        Iterator<E> iterator = QueryUtils.prepareTerminalOperation(this, true);
        Double sum = null;
        while (iterator.hasNext()) {
            E next = iterator.next();
            if (next == null) {
                continue;
            }
            if (sum != null) {
                sum += next.doubleValue();
            } else {
                sum = next.doubleValue();
            }
        }
        return Optional.ofNullable(sum);
    }

    @Override
    public Optional<Long> sumAsLong() {
        Iterator<E> iterator = QueryUtils.prepareTerminalOperation(this, true);
        Long sum = null;
        while (iterator.hasNext()) {
            E next = iterator.next();
            if (next == null) {
                continue;
            }
            if (sum != null) {
                sum += next.longValue();
            } else {
                sum = next.longValue();
            }
        }
        return Optional.ofNullable(sum);
    }

    @Override
    public Optional<Double> average() {
        Iterator<E> iterator = QueryUtils.prepareTerminalOperation(this, true);
        Double sum = null;
        long count = 0L;
        while (iterator.hasNext()) {
            count++;
            E next = iterator.next();
            if (next == null) {
                continue;
            }
            if (sum != null) {
                sum += next.doubleValue();
            } else {
                sum = next.doubleValue();
            }
        }
        if (count == 0) {
            return Optional.empty();
        }
        return Optional.of(sum / count);
    }

    @Override
    public Optional<Double> maxAsDouble() {
        Iterator<E> iterator = QueryUtils.prepareTerminalOperation(this, true);
        Double max = null;
        while (iterator.hasNext()) {
            E element = iterator.next();
            if (element == null) {
                continue;
            }
            if (max == null || element.doubleValue() > max) {
                max = element.doubleValue();
            }
        }
        return Optional.ofNullable(max);
    }

    @Override
    public Optional<Long> maxAsLong() {
        Iterator<E> iterator = QueryUtils.prepareTerminalOperation(this, true);
        Long max = null;
        while (iterator.hasNext()) {
            E element = iterator.next();
            if (element == null) {
                continue;
            }
            if (max == null || element.longValue() > max) {
                max = element.longValue();
            }
        }
        return Optional.ofNullable(max);
    }

    @Override
    public Optional<Double> minAsDouble() {
        Iterator<E> iterator = QueryUtils.prepareTerminalOperation(this, true);
        Double min = null;
        while (iterator.hasNext()) {
            E element = iterator.next();
            if (element == null) {
                continue;
            }
            if (min == null || element.doubleValue() < min) {
                min = element.doubleValue();
            }
        }
        return Optional.ofNullable(min);
    }

    @Override
    public Optional<Long> minAsLong() {
        Iterator<E> iterator = QueryUtils.prepareTerminalOperation(this, true);
        Long min = null;
        while (iterator.hasNext()) {
            E element = iterator.next();
            if (element == null) {
                continue;
            }
            if (min == null || element.longValue() < min) {
                min = element.longValue();
            }
        }
        return Optional.ofNullable(min);
    }

    @Override
    public NumericQueryStepBuilder<S, Long> round() {
        return new NumericQueryRoundStepBuilder<>(this);
    }

    @Override
    public NumericQueryStepBuilder<S, Integer> roundToInt() {
        return new NumericQueryRoundToIntStepBuilder<>(this);
    }

    @Override
    public NumericQueryStepBuilder<S, Long> floor() {
        return new NumericQueryFloorStepBuilder<>(this);
    }

    @Override
    public NumericQueryStepBuilder<S, Long> ceil() {
        return new NumericQueryCeilStepBuilder<>(this);
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


}
