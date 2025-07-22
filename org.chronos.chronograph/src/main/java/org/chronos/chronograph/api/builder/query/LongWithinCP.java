package org.chronos.chronograph.api.builder.query;

import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.BiPredicate;

public class LongWithinCP implements PBiPredicate<Object, Collection> {

    public LongWithinCP() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean test(final Object o, final Collection collection) {
        if(collection == null || collection.isEmpty()){
            return false;
        }
        if(o instanceof Long){
            return collection.contains(o);
        }else if(o instanceof Collection){
            for(Object element : (Collection<Object>)o){
                if(test(element, collection)){
                    return true;
                }
            }
        }
        return false;
    }

    @NotNull
    @Override
    public PBiPredicate<Object, Collection> negate() {
        return new LongWithoutCP();
    }

    @Override
    public int hashCode(){
        return this.getClass().hashCode();
    }

    @Override
    public boolean equals(Object other){
        if(other == null){
            return false;
        }
        return other instanceof LongWithinCP;
    }

    @Override
    public String toString() {
        return "Long Within";
    }
}
