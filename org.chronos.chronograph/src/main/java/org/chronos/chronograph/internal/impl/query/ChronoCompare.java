package org.chronos.chronograph.internal.impl.query;

import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.apache.tinkerpop.gremlin.util.NumberHelper;

import java.util.Collection;
import java.util.Objects;
import java.util.function.BiPredicate;

public enum ChronoCompare implements PBiPredicate<Object, Object> {

    EQ {
        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::compareAtomic);
        }

        @Override
        public ChronoCompare negate() {
            return NEQ;
        }

        private boolean compareAtomic(Object left, Object right){
            if(left == null || right == null){
                return false;
            }
            return Objects.equals(left, right);
        }
    },

    NEQ {

        @Override
        public boolean test(final Object o, final Object o2) {
            return !EQ.test(o, o2);
        }

        @Override
        public ChronoCompare negate() {
            return EQ;
        }

    },

    GT {

        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::compareAtomic);
        }

        @Override
        public ChronoCompare negate() {
            return LTE;
        }

        @SuppressWarnings({"unchecked"})
        private boolean compareAtomic(Object first, Object second){
            if(first == null || second == null){
                return false;
            }
            if(first instanceof Number && second instanceof Number){
                return NumberHelper.compare((Number) first, (Number) second) > 0;
            }
            if(first instanceof Comparable){
                return ((Comparable<Object>)first).compareTo(second) > 0;
            }
            return false;
        }

    },

    GTE {

        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::compareAtomic);
        }

        @Override
        public ChronoCompare negate() {
            return LT;
        }

        @SuppressWarnings({"unchecked"})
        private boolean compareAtomic(Object first, Object second){
            if(first == null || second == null){
                return false;
            }
            if(first instanceof Number && second instanceof Number){
                return NumberHelper.compare((Number) first, (Number) second) >= 0;
            }
            if(first instanceof Comparable){
                return ((Comparable<Object>)first).compareTo(second) >= 0;
            }
            return false;
        }

    },

    LT {

        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::compareAtomic);
        }

        @Override
        public ChronoCompare negate() {
            return GTE;
        }

        @SuppressWarnings({"unchecked"})
        private boolean compareAtomic(Object first, Object second){
            if(first == null || second == null){
                return false;
            }
            if(first instanceof Number && second instanceof Number){
                return NumberHelper.compare((Number) first, (Number) second) < 0;
            }
            if(first instanceof Comparable){
                return ((Comparable<Object>)first).compareTo(second) < 0;
            }
            return false;
        }

    },

    LTE {


        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::compareAtomic);
        }

        @Override
        public ChronoCompare negate() {
            return GT;
        }

        @SuppressWarnings({"unchecked"})
        private boolean compareAtomic(Object first, Object second){
            if(first == null || second == null){
                return false;
            }
            if(first instanceof Number && second instanceof Number){
                return NumberHelper.compare((Number) first, (Number) second) <= 0;
            }
            if(first instanceof Comparable){
                return ((Comparable<Object>)first).compareTo(second) <= 0;
            }
            return false;
        }

    },

    WITHIN{

        @Override
        public boolean test(final Object o, final Object o2) {
            if(o == null || o2 == null){
                return false;
            }
            Collection<Object> left = ChronoCompareUtil.liftToCollection(o);
            Collection<Object> right = ChronoCompareUtil.liftToCollection(o2);
            for(Object leftElement : left){
                if(right.contains(leftElement)){
                    return true;
                }
            }
            return false;
        }

        @Override
        public ChronoCompare negate() {
            return WITHOUT;
        }
    },

    WITHOUT{

        @Override
        public boolean test(final Object o, final Object o2) {
            return !WITHIN.test(o, o2);
        }

        @Override
        public ChronoCompare negate() {
            return WITHIN;
        }
    };

    public abstract ChronoCompare negate();

}
