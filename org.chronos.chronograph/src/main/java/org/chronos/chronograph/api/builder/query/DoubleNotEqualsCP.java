package org.chronos.chronograph.api.builder.query;

import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiPredicate;

public class DoubleNotEqualsCP implements PBiPredicate<Object, Object> {

    private final double tolerance;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public DoubleNotEqualsCP(final double tolerance) {
        this.tolerance = tolerance;
    }

    public double getTolerance() {
        return this.tolerance;
    }

    @Override
    public boolean test(final Object o, final Object o2) {
        return !this.negate().test(o, o2);
    }


    @NotNull
    @Override
    public PBiPredicate<Object, Object> negate() {
        return new DoubleEqualsCP(this.tolerance);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DoubleNotEqualsCP that = (DoubleNotEqualsCP) o;

        return Double.compare(that.tolerance, tolerance) == 0;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(tolerance);
    }

    @Override
    public String toString() {
        return "Double Neq";
    }

}
