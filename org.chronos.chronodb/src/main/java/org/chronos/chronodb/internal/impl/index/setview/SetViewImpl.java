package org.chronos.chronodb.internal.impl.index.setview;

import com.google.common.collect.Sets;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class SetViewImpl<T> extends AbstractSet<T> implements SetView<T> {


    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final Set<T> data;
    private final int sizeMin;
    private final int sizeMax;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    protected SetViewImpl(Set<T> data, int sizeMin, int sizeMax) {
        this.data = data;
        this.sizeMin = sizeMin;
        this.sizeMax = sizeMax;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    public int maxSize() {
        return sizeMax;
    }

    public int minSize() {
        return sizeMin;
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
    public boolean contains(final Object o) {
        return data.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return data.iterator();
    }

    @Override
    public Object[] toArray() {
        return data.toArray();
    }

    @Override
    public <T1> T1[] toArray(final T1[] a) {
        return data.toArray(a);
    }

    @Override
    public boolean add(final T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(final Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        return this.data.containsAll(c);
    }

    @Override
    public boolean addAll(final Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }


}
