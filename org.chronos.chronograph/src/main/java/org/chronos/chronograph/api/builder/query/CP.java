package org.chronos.chronograph.api.builder.query;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronograph.internal.impl.query.ChronoCompare;
import org.chronos.chronograph.internal.impl.query.ChronoStringCompare;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

public class CP<V> extends P<V> {

    protected CP(final PBiPredicate<V, V> biPredicate, final V value) {
        super(biPredicate, value);
    }

    public static <V> CP<V> cEq(Object value) {
        if(value instanceof Collection){
            if(((Collection<?>)value).isEmpty()){
                // gremlin standard: if an empty collection is passed
                // to "eq" it means that the predicate matches NOTHING.
                return new CP(ChronoCompare.EQ, Collections.emptySet());
            }
            // in general, collection values are not allowed.
            throw new IllegalArgumentException("Error in has clause: 'equals' with a collection value is not supported! If required, use .filter(...) with a lambda expression instead.");
        }
        return new CP(ChronoCompare.EQ, value);
    }

    public static <V> CP<V> cNeq(Object value) {
        if(value instanceof Collection){
            throw new IllegalArgumentException("Error in has clause: 'not equals' with a collection value is not supported! If required, use .filter(...) with a lambda expression instead.");
        }
        return new CP(ChronoCompare.NEQ, value);
    }

    public static <V> CP<V> cGt(Object value) {
        if(value instanceof Collection){
            throw new IllegalArgumentException("Error in has clause: 'greater than' with a collection value is not supported! If required, use .filter(...) with a lambda expression instead.");
        }
        return new CP(ChronoCompare.GT, value);
    }

    public static <V> CP<V> cGte(Object value) {
        if(value instanceof Collection){
            throw new IllegalArgumentException("Error in has clause: 'greater than or equal to' with a collection value is not supported! If required, use .filter(...) with a lambda expression instead.");
        }
        return new CP(ChronoCompare.GTE, value);
    }

    public static <V> CP<V> cLt(Object value) {
        if(value instanceof Collection){
            throw new IllegalArgumentException("Error in has clause: 'less than' with a collection value is not supported! If required, use .filter(...) with a lambda expression instead.");
        }
        return new CP(ChronoCompare.LT, value);
    }

    public static <V> CP<V> cLte(Object value) {
        if(value instanceof Collection){
            throw new IllegalArgumentException("Error in has clause: 'less than or equal to' with a collection value is not supported! If required, use .filter(...) with a lambda expression instead.");
        }
        return new CP(ChronoCompare.LTE, value);
    }

    public static <V> CP<V> cWithin(final V... values) {
        for(V value : values){
            if(value instanceof Collection){
                throw new IllegalArgumentException("Error in has clause: 'within' with nested collection values is not supported! If required, use .filter(...) with a lambda expression instead.");
            }
        }
        return CP.cWithin(Arrays.asList(values));
    }

    public static <V> CP<V> cWithin(final Collection<V> value) {
        for(V item : value){
            if(item instanceof Collection){
                throw new IllegalArgumentException("Error in has clause: 'within' with nested collection values is not supported! If required, use .filter(...) with a lambda expression instead.");
            }
        }
        return new CP(ChronoCompare.WITHIN, value);
    }

    public static <V> CP<V> cWithout(final V... values) {
        for(V value : values){
            if(value instanceof Collection){
                throw new IllegalArgumentException("Error in has clause: 'without' with nested collection values is not supported! If required, use .filter(...) with a lambda expression instead.");
            }
        }
        return CP.cWithout(Arrays.asList(values));
    }

    public static <V> CP<V> cWithout(final Collection<V> value) {
        for(V item : value){
            if(item instanceof Collection){
                throw new IllegalArgumentException("Error in has clause: 'without' with nested collection values is not supported! If required, use .filter(...) with a lambda expression instead.");
            }
        }
        return new CP(ChronoCompare.WITHOUT, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> eqIgnoreCase(String value){
        return new CP(ChronoStringCompare.STRING_EQUALS_IGNORE_CASE, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> neqIgnoreCase(String value){
        return new CP(ChronoStringCompare.STRING_NOT_EQUALS_IGNORE_CASE, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> startsWith(String value){
        return new CP(ChronoStringCompare.STRING_STARTS_WITH, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> notStartsWith(String value){
        return new CP(ChronoStringCompare.STRING_NOT_STARTS_WITH, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> startsWithIgnoreCase(String value){
        return new CP(ChronoStringCompare.STRING_STARTS_WITH_IGNORE_CASE, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> notStartsWithIgnoreCase(String value){
        return new CP(ChronoStringCompare.STRING_NOT_STARTS_WITH_IGNORE_CASE, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> endsWith(String value){
        return new CP(ChronoStringCompare.STRING_ENDS_WITH, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> notEndsWith(String value){
        return new CP(ChronoStringCompare.STRING_NOT_ENDS_WITH, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> endsWithIgnoreCase(String value){
        return new CP(ChronoStringCompare.STRING_ENDS_WITH_IGNORE_CASE, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> notEndsWithIgnoreCase(String value){
        return new CP(ChronoStringCompare.STRING_NOT_ENDS_WITH_IGNORE_CASE, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> contains(String value){
        return new CP(ChronoStringCompare.STRING_CONTAINS, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> notContains(String value){
        return new CP(ChronoStringCompare.STRING_NOT_CONTAINS, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> containsIgnoreCase(String value){
        return new CP(ChronoStringCompare.STRING_CONTAINS_IGNORE_CASE, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> notContainsIgnoreCase(String value){
        return new CP(ChronoStringCompare.STRING_NOT_CONTAINS_IGNORE_CASE, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> matchesRegex(String value){
        return new CP(ChronoStringCompare.STRING_MATCHES_REGEX, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> matchesRegexIgnoreCase(String value){
        return new CP(ChronoStringCompare.STRING_MATCHES_REGEX_IGNORE_CASE, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> notMatchesRegex(String value){
        return new CP(ChronoStringCompare.STRING_NOT_MATCHES_REGEX, value);
    }

    @SuppressWarnings("unchecked")
    public static CP<String> notMatchesRegexIgnoreCase(String value){
        return new CP(ChronoStringCompare.STRING_NOT_MATCHES_REGEX_IGNORE_CASE, value);
    }

    public static CP<String> withinIgnoreCase(String... values){
        return withinIgnoreCase(Lists.newArrayList(values));
    }

    @SuppressWarnings("unchecked")
    public static CP<String> withinIgnoreCase(Collection<String> values){
        return new CP(new StringWithinCP(TextMatchMode.CASE_INSENSITIVE), values.stream().map(String::toLowerCase).collect(Collectors.toSet()));
    }

    public static CP<String> withoutIgnoreCase(String... values){
        return withoutIgnoreCase(Lists.newArrayList(values));
    }

    @SuppressWarnings("unchecked")
    public static CP<String> withoutIgnoreCase(Collection<String> values){
        return new CP(new StringWithoutCP(TextMatchMode.CASE_INSENSITIVE), values.stream().map(String::toLowerCase).collect(Collectors.toSet()));
    }

    @SuppressWarnings("unchecked")
    public static CP<Double> within(Collection<Double> values, double tolerance){
        return new CP(new DoubleWithinCP(tolerance), Sets.newHashSet(values));
    }

    @SuppressWarnings("unchecked")
    public static CP<Double> without(Collection<Double> values, double tolerance){
        return new CP(new DoubleWithoutCP(tolerance), Sets.newHashSet(values));
    }

    @SuppressWarnings("unchecked")
    public static CP<Double> eq(Double value, double tolerance){
        return new CP(new DoubleEqualsCP(tolerance), value);
    }

    @SuppressWarnings("unchecked")
    public static CP<Double> neq(Double value, double tolerance){
        return new CP(new DoubleNotEqualsCP(tolerance), value);
    }
}
