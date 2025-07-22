package org.chronos.chronograph.internal.impl.query;

import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;

import java.util.function.BiPredicate;
import java.util.regex.Pattern;

public enum ChronoStringCompare implements PBiPredicate<Object, Object> {

    STRING_EQUALS_IGNORE_CASE {
        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::compareAtomic);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_NOT_EQUALS_IGNORE_CASE;
        }

        private boolean compareAtomic(Object left, Object right) {
            if (left instanceof String == false) {
                return false;
            }
            if (right instanceof String == false) {
                return false;
            }
            String sLeft = (String) left;
            String sRight = (String) right;
            return sLeft.equalsIgnoreCase(sRight);
        }

    },

    STRING_NOT_EQUALS_IGNORE_CASE {
        @Override
        public boolean test(final Object o, final Object o2) {
            return !STRING_EQUALS_IGNORE_CASE.test(o, o2);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_EQUALS_IGNORE_CASE;
        }

    },

    STRING_STARTS_WITH {

        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::testAtomic);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_NOT_STARTS_WITH;
        }

        private boolean testAtomic(final Object o, final Object o2) {
            if (o == null || o2 == null) {
                return false;
            }
            if (!(o instanceof String) || !(o2 instanceof String)) {
                return false;
            }
            String string1 = (String) o;
            String string2 = (String) o2;
            return string1.startsWith(string2);
        }

    },

    STRING_NOT_STARTS_WITH {

        @Override
        public boolean test(final Object o, final Object o2) {
            return !STRING_STARTS_WITH.test(o, o2);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_STARTS_WITH;
        }

    },

    STRING_STARTS_WITH_IGNORE_CASE {

        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::testAtomic);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_NOT_STARTS_WITH_IGNORE_CASE;
        }

        private boolean testAtomic(final Object o, final Object o2) {
            if (o == null || o2 == null) {
                return false;
            }
            if (!(o instanceof String) || !(o2 instanceof String)) {
                return false;
            }
            String string1 = (String) o;
            String string2 = (String) o2;
            return string1.toLowerCase().startsWith(string2.toLowerCase());
        }
    },

    STRING_NOT_STARTS_WITH_IGNORE_CASE {

        @Override
        public boolean test(final Object o, final Object o2) {
            return !STRING_STARTS_WITH_IGNORE_CASE.test(o, o2);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_STARTS_WITH_IGNORE_CASE;
        }

    },

    STRING_ENDS_WITH {

        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::testAtomic);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_NOT_ENDS_WITH;
        }

        private boolean testAtomic(final Object o, final Object o2) {
            if (o == null || o2 == null) {
                return false;
            }
            if (!(o instanceof String) || !(o2 instanceof String)) {
                return false;
            }
            String string1 = (String) o;
            String string2 = (String) o2;
            return string1.endsWith(string2);
        }

    },

    STRING_NOT_ENDS_WITH {

        @Override
        public boolean test(final Object o, final Object o2) {
            return !STRING_ENDS_WITH.test(o, o2);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_ENDS_WITH;
        }

    },

    STRING_ENDS_WITH_IGNORE_CASE {

        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::testAtomic);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_NOT_ENDS_WITH_IGNORE_CASE;
        }

        private boolean testAtomic(final Object o, final Object o2) {
            if (o == null || o2 == null) {
                return false;
            }
            if (!(o instanceof String) || !(o2 instanceof String)) {
                return false;
            }
            String string1 = (String) o;
            String string2 = (String) o2;
            return string1.toLowerCase().endsWith(string2.toLowerCase());
        }

    },

    STRING_NOT_ENDS_WITH_IGNORE_CASE {

        @Override
        public boolean test(final Object o, final Object o2) {
            return !STRING_ENDS_WITH_IGNORE_CASE.test(o, o2);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_ENDS_WITH_IGNORE_CASE;
        }

    },

    STRING_CONTAINS {

        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::testAtomic);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_NOT_CONTAINS;
        }

        private boolean testAtomic(final Object o, final Object o2) {
            if (o == null || o2 == null) {
                return false;
            }
            if (!(o instanceof String) || !(o2 instanceof String)) {
                return false;
            }
            String string1 = (String) o;
            String string2 = (String) o2;
            return string1.contains(string2);
        }

    },

    STRING_NOT_CONTAINS {

        @Override
        public boolean test(final Object o, final Object o2) {
            return !STRING_CONTAINS.test(o, o2);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_CONTAINS;
        }

    },

    STRING_CONTAINS_IGNORE_CASE {

        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::testAtomic);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_NOT_CONTAINS_IGNORE_CASE;
        }

        private boolean testAtomic(final Object o, final Object o2) {
            if (o == null || o2 == null) {
                return false;
            }
            if (!(o instanceof String) || !(o2 instanceof String)) {
                return false;
            }
            String string1 = (String) o;
            String string2 = (String) o2;
            return string1.toLowerCase().contains(string2.toLowerCase());
        }
    },

    STRING_NOT_CONTAINS_IGNORE_CASE {

        @Override
        public boolean test(final Object o, final Object o2) {
            return !STRING_CONTAINS_IGNORE_CASE.test(o, o2);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_CONTAINS_IGNORE_CASE;
        }

    },

    STRING_MATCHES_REGEX {

        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::testAtomic);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_NOT_MATCHES_REGEX;
        }

        private boolean testAtomic(final Object o, final Object o2) {
            if (o == null || o2 == null) {
                return false;
            }
            if (!(o instanceof String) || !(o2 instanceof String)) {
                return false;
            }
            String string1 = (String) o;
            String string2 = (String) o2;
            return string1.matches(string2);
        }
    },

    STRING_NOT_MATCHES_REGEX {

        @Override
        public boolean test(final Object o, final Object o2) {
            return !STRING_MATCHES_REGEX.test(o, o2);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_MATCHES_REGEX;
        }

    },

    STRING_MATCHES_REGEX_IGNORE_CASE {

        @Override
        public boolean test(final Object o, final Object o2) {
            return ChronoCompareUtil.compare(o, o2, this::testAtomic);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_NOT_MATCHES_REGEX_IGNORE_CASE;
        }

        private boolean testAtomic(final Object o, final Object o2) {
            if (o == null || o2 == null) {
                return false;
            }
            if (!(o instanceof String) || !(o2 instanceof String)) {
                return false;
            }
            String string1 = (String) o;
            String string2 = (String) o2;
            return Pattern.compile(string2, Pattern.CASE_INSENSITIVE).matcher(string1).matches();
        }

    },

    STRING_NOT_MATCHES_REGEX_IGNORE_CASE {

        @Override
        public boolean test(final Object o, final Object o2) {
            return !STRING_MATCHES_REGEX_IGNORE_CASE.test(o, o2);
        }

        @Override
        public ChronoStringCompare negate() {
            return STRING_MATCHES_REGEX_IGNORE_CASE;
        }

    };

    public abstract ChronoStringCompare negate();

}
