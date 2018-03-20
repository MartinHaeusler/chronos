package org.chronos.chronodb.internal.impl.index.setview;

import com.google.common.collect.Sets;

import java.util.Set;

public interface SetView<T> extends Set<T> {

    // =================================================================================================================
    // STATIC HELPERS
    // =================================================================================================================

    public static int estimateSizeOf(Set<?> set){
        if(set instanceof SetView){
            return ((SetView<?>)set).estimatedSize();
        }else{
            return set.size();
        }
    }

    // =================================================================================================================
    // STATIC FACTORIES
    // =================================================================================================================

    public static <T> SetView<T> intersection(Set<T> a, Set<T> b){
        int sizeLeft = estimateSizeOf(a);
        int sizeRight = estimateSizeOf(b);
        Set<T> data;
        if (sizeLeft < sizeRight) {
            data = Sets.intersection(a, b);
        } else {
            data = Sets.intersection(b, a);
        }
        // pessimistic: the minimum size of the result of a
        // set intersection is the empty set.
        int sizeMin = 0;
        // fact: the maximum size of the result of a
        // set intersection is the minimum of the two sizes
        // of the origin sets (a set cannot grow larger by
        // intersecting it with another set)
        int sizeMax = Math.min(sizeLeft, sizeRight);
        return new SetViewImpl<>(data, sizeMin, sizeMax);
    }

    public static <T> SetView<T> union(Set<T> a, Set<T> b){
        int sizeLeft = estimateSizeOf(a);
        int sizeRight = estimateSizeOf(b);
        Set<T> data = Sets.union(a, b);
        // the minimum size of a set union is the size of the
        // larger of the two input sets. This is the case if
        // one set entirely contains the other.
        int sizeMin = Math.max(sizeLeft, sizeRight);

        // the maximum size of a set union is the sum of the
        // two input set sizes. This is the case if the sets
        // are disjoint (contain no common element).
        int sizeMax = sizeLeft + sizeRight;

        return new SetViewImpl<>(data, sizeMin, sizeMax);
    }



    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    public int maxSize();

    public int minSize();

    public default int estimatedSize(){
        return (maxSize() + minSize()) / 2;
    }


}
