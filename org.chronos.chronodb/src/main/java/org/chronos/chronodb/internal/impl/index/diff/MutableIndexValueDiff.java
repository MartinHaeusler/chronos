package org.chronos.chronodb.internal.impl.index.diff;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class MutableIndexValueDiff implements IndexValueDiff {

	private final Object oldValue;
	private final Object newValue;

	private SetMultimap<String, Object> indexNameToAdditions;
	private SetMultimap<String, Object> indexNameToRemovals;

	public MutableIndexValueDiff(final Object oldValue, final Object newValue) {
		this.oldValue = oldValue;
		this.newValue = newValue;
	}

	@Override
	public Object getOldValue() {
		return this.oldValue;
	}

	@Override
	public Object getNewValue() {
		return this.newValue;
	}

	@Override
	public Set<Object> getAdditions(final String indexName) {
		if (this.indexNameToAdditions == null || this.indexNameToAdditions.isEmpty()) {
			return Collections.emptySet();
		}
		return Collections.unmodifiableSet(this.indexNameToAdditions.get(indexName));
	}

	@Override
	public Set<Object> getRemovals(final String indexName) {
		if (this.indexNameToRemovals == null || this.indexNameToRemovals.isEmpty()) {
			return Collections.emptySet();
		}
		return Collections.unmodifiableSet(this.indexNameToRemovals.get(indexName));
	}

	@Override
	public Set<String> getChangedIndices() {
		if (this.isEmpty()) {
			return Collections.emptySet();
		}
		if (this.indexNameToAdditions == null || this.indexNameToAdditions.isEmpty()) {
			return Collections.unmodifiableSet(this.indexNameToRemovals.keySet());
		}
		if (this.indexNameToRemovals == null || this.indexNameToRemovals.isEmpty()) {
			return Collections.unmodifiableSet(this.indexNameToAdditions.keySet());
		}
		Set<String> changedIndices = Sets.union(this.indexNameToAdditions.keySet(), this.indexNameToRemovals.keySet());
		return Collections.unmodifiableSet(changedIndices);
	}

	@Override
	public boolean isEmpty() {
		// both change sets are either NULL (never touched) or empty (touched and then cleared)
		if ((this.indexNameToAdditions == null || this.indexNameToAdditions.isEmpty())
				&& (this.indexNameToRemovals == null || this.indexNameToRemovals.isEmpty())) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean isAdditive() {
		if (this.indexNameToAdditions != null && this.indexNameToAdditions.size() > 0) {
			if (this.indexNameToRemovals == null || this.indexNameToRemovals.isEmpty()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isSubtractive() {
		if (this.indexNameToRemovals != null && this.indexNameToRemovals.size() > 0) {
			if (this.indexNameToAdditions == null || this.indexNameToAdditions.isEmpty()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isMixed() {
		if (this.indexNameToAdditions != null && this.indexNameToAdditions.size() > 0) {
			if (this.indexNameToRemovals != null && this.indexNameToRemovals.size() > 0) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isIndexChanged(final String indexName) {
		if (this.indexNameToAdditions != null && this.indexNameToAdditions.containsKey(indexName)) {
			return true;
		} else if (this.indexNameToRemovals != null && this.indexNameToRemovals.containsKey(indexName)) {
			return true;
		}
		return false;
	}

	public void add(final String index, final Object value) {
		checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		if (this.isEntryRemoval()) {
			throw new IllegalStateException(
					"Cannot insert additive diff values to a diff that represents an entry removal!");
		}
		if (this.indexNameToAdditions == null) {
			this.indexNameToAdditions = HashMultimap.create();
		}
		this.indexNameToAdditions.put(index, value);
		if (this.indexNameToRemovals != null) {
			this.indexNameToRemovals.remove(index, value);
		}
	}

	public void add(final String index, final Set<String> values) {
		checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
		if (values == null || values.isEmpty()) {
			return;
		}
		for (String value : values) {
			this.add(index, value);
		}
	}

	public void removeSingleValue(final String index, final Object value) {
		checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		if (this.isEntryAddition()) {
			throw new IllegalStateException(
					"Cannot insert subtractive diff values to a diff that represents an entry addition!");
		}
		if (this.indexNameToRemovals == null) {
			this.indexNameToRemovals = HashMultimap.create();
		}
		this.indexNameToRemovals.put(index, value);
		if (this.indexNameToAdditions != null) {
			this.indexNameToAdditions.remove(index, value);
		}
	}

	public void removeMultipleValues(final String index, final Set<Object> values) {
		checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
		if (values == null || values.isEmpty()) {
			return;
		}
		for (Object value : values) {
			this.removeSingleValue(index, value);
		}
	}

}
