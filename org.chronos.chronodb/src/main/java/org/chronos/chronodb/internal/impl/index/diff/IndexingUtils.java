package org.chronos.chronodb.internal.impl.index.diff;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.api.exceptions.IndexerConflictException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class IndexingUtils {

	/**
	 * Calculates a diff between the indexed values of the given <code>oldValue</code> and <code>newValue</code>.
	 *
	 * @param indexNameToIndexers
	 *            A mapping from index names to indexers. This method will attempt to resolve the index value(s) for all index names. May be empty, in which case the resulting diff will also be empty. Must not be <code>null</code>.
	 * @param oldValue
	 *            The old (previous) value object to analyze. May be <code>null</code> to indicate that the <code>newValue</code> is an addition.
	 * @param newValue
	 *            The new value object to analyze. May be <code>null</code> to indicate that the <code>oldValue</code> was deleted.
	 *
	 * @return The diff between the old and new index values. Never <code>null</code>, may be empty.
	 */
	public static IndexValueDiff calculateDiff(final SetMultimap<String, ChronoIndexer> indexNameToIndexers,
			final Object oldValue, final Object newValue) {
		checkNotNull(indexNameToIndexers, "Precondition violation - argument 'indexNameToIndexers' must not be NULL!");
		MutableIndexValueDiff diff = new MutableIndexValueDiff(oldValue, newValue);
		if (indexNameToIndexers.isEmpty() || oldValue == null && newValue == null) {
			// return the empty diff
			return diff;
		}
		// iterate over the known indices
		for (String indexName : indexNameToIndexers.keySet()) {
			Set<ChronoIndexer> indexers = indexNameToIndexers.get(indexName);
			// prepare the sets of values for that index
			Set<String> oldValues = getIndexedValuesForObject(indexers, oldValue);
			Set<String> newValues = getIndexedValuesForObject(indexers, newValue);
			// calculate the set differences
			Set<String> addedValues = Sets.newHashSet();
			addedValues.addAll(newValues);
			addedValues.removeAll(oldValues);
			Set<String> removedValues = Sets.newHashSet();
			removedValues.addAll(oldValues);
			removedValues.removeAll(newValues);
			// update the diff
			for (String addedValue : addedValues) {
				if (addedValue == null || addedValue.trim().isEmpty()) {
					// don't create documents for empty values
					continue;
				}
				diff.add(indexName, addedValue);
			}
			diff.remove(indexName, removedValues);
		}
		return diff;
	}

	/**
	 * Returns the indexed values for the given object.
	 *
	 * @param indexNameToIndexers
	 *            A multimap from index name to indexers. Must not be <code>null</code>, may be empty, in which case the resulting multimap will be empty.
	 * @param object
	 *            The object to get the index values for. May be <code>null</code>, in which case the resulting multimap will be empty.
	 *
	 * @return The mapping from index name to indexed values. May be empty, but never <code>null</code>.
	 */
	public static SetMultimap<String, String> getIndexedValuesForObject(
			final SetMultimap<String, ChronoIndexer> indexNameToIndexers, final Object object) {
		checkNotNull(indexNameToIndexers, "Precondition violation - argument 'indexNameToIndexers' must not be NULL!");
		SetMultimap<String, String> indexValuesMap = HashMultimap.create();
		if (object == null || indexNameToIndexers.isEmpty()) {
			return indexValuesMap;
		}
		for (String indexName : indexNameToIndexers.keySet()) {
			Set<ChronoIndexer> indexers = indexNameToIndexers.get(indexName);
			Set<String> indexValues = getIndexedValuesForObject(indexers, object);
			if (indexValues.isEmpty()) {
				continue;
			}
			indexValuesMap.putAll(indexName, indexValues);
		}
		return indexValuesMap;
	}

	/**
	 * Returns the indexed values for the given object.
	 *
	 * @param indexers
	 *            The set of indexers to run on the object. Must not be <code>null</code>. Only one of the indexers may produce an index value, otherwise an indexer conflict occurs. Usually, all of these indexers correspond to the same index name.
	 *
	 * @param object
	 *            The object to index. May be <code>null</code>, which results in the empty set as the output.
	 *
	 * @return An immutable set, containing the index values calculated by running the given indexers on the given object. May be empty, but never <code>null</code>.
	 */
	public static Set<String> getIndexedValuesForObject(final Set<ChronoIndexer> indexers, final Object object) {
		checkNotNull(indexers, "Precondition violation - argument 'indexers' must not be NULL!");
		if (object == null) {
			// when the object to index is null, we can't produce any indexed values.
			// empty set is already unmodifiable, no need to wrap it in Collections.umodifiableSet.
			return Collections.emptySet();
		}
		Set<String> indexedValues = null;
		for (ChronoIndexer indexer : indexers) {
			if (indexer.canIndex(object)) {
				if (indexedValues != null) {
					// we have a conflict - another indexer has already identified the new index value!
					throw new IndexerConflictException(
							"There are multiple indexers capable of indexing an object of class '"
									+ object.getClass().getName() + "' on the same index!");
				} else {
					indexedValues = indexer.getIndexValues(object);
					if (indexedValues == null) {
						// indexer returned a NULL set; replace it with empty set
						indexedValues = Collections.emptySet();
					} else {
						// make sure that there are no NULL values or empty values in the indexed values set
						indexedValues = indexedValues.stream().filter(e -> e != null && e.trim().isEmpty() == false)
								.collect(Collectors.toSet());
					}
				}
			}
		}
		if (indexedValues == null) {
			// no indexer could index this element
			return Collections.emptySet();
		}
		return Collections.unmodifiableSet(indexedValues);
	}

}
