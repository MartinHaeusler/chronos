package org.chronos.chronograph.api.builder.query;

import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.BiPredicate;

public class DoubleWithoutCP implements PBiPredicate<Object, Collection> {

    private final double tolerance;

    public DoubleWithoutCP(final double tolerance) {
        this.tolerance = tolerance;
    }

    public double getTolerance() {
        return tolerance;
    }

    @Override
    public boolean test(final Object o, final Collection collection) {
        return !this.negate().test(o, collection);
    }

    @NotNull
    @Override
    public PBiPredicate<Object, Collection> negate() {
        return new DoubleWithinCP(this.tolerance);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DoubleWithoutCP that = (DoubleWithoutCP) o;

        return Double.compare(that.tolerance, tolerance) == 0;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(tolerance);
    }

    @Override
    public String toString() {
        return "Double Without";
    }
}
