package org.chronos.chronograph.api.builder.query;

import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.chronos.chronograph.internal.impl.query.ChronoCompareUtil;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiPredicate;

public class DoubleEqualsCP implements PBiPredicate<Object, Object> {

    private final double tolerance;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public DoubleEqualsCP(final double tolerance) {
        this.tolerance = tolerance;
    }

    public double getTolerance() {
        return this.tolerance;
    }

    @Override
    public boolean test(final Object o, final Object o2) {
        return ChronoCompareUtil.compare(o, o2, this::testAtomic);
    }

    private boolean testAtomic(final Object o, final Object o2){
        if(o instanceof Number == false || o2 instanceof Number == false){
            return false;
        }
        double left = ((Number)o).doubleValue();
        double right = ((Number)o2).doubleValue();
        return Math.abs(left - right) < this.tolerance;
    }


    @NotNull
    @Override
    public PBiPredicate<Object, Object> negate() {
        return new DoubleNotEqualsCP(this.tolerance);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DoubleEqualsCP that = (DoubleEqualsCP) o;

        return Double.compare(that.tolerance, tolerance) == 0;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(tolerance);
    }

    @Override
    public String toString() {
        return "Double Eq";
    }

}
