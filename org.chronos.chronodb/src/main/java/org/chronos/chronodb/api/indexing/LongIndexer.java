package org.chronos.chronodb.api.indexing;

import java.util.Set;

import org.chronos.chronodb.api.ChronoDB;

/**
 * A {@link LongIndexer} is an {@link Indexer} capable of extracting {@link Long} values from an object.
 *
 * <p>
 * Please refer to the documentation in {@link Indexer} for more details.
 *
 * @author martin.haeulser@uibk.ac.at -- Initial Contribution and API
 */
public interface LongIndexer extends Indexer<Long> {

	/**
	 * Produces the indexed long values of the given object.
	 *
	 * <p>
	 * This method is only called by {@link ChronoDB} if a previous call to {@link #canIndex(Object)} with the same parameter returned <code>true</code>.
	 *
	 * @param object
	 *            The object to index. Must not be <code>null</code>.
	 * @return A {@link Set} of longs representing the indexed values for the given object. <code>null</code>-values in the set will be ignored. If this method returns <code>null</code>, it will be treated as if it had returned an empty set.
	 */
	@Override
	public Set<Long> getIndexValues(Object object);
}
