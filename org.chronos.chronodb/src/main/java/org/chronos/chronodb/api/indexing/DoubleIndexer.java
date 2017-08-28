package org.chronos.chronodb.api.indexing;

import java.util.Set;

import org.chronos.chronodb.api.ChronoDB;

/**
 * A {@link DoubleIndexer} is an {@link Indexer} capable of extracting {@link Double} values from an object.
 *
 * <p>
 * Please refer to the documentation in {@link Indexer} for more details.
 *
 * @author martin.haeulser@uibk.ac.at -- Initial Contribution and API
 */
public interface DoubleIndexer extends Indexer<Double> {

	/**
	 * Produces the indexed Double values of the given object.
	 *
	 * <p>
	 * This method is only called by {@link ChronoDB} if a previous call to {@link #canIndex(Object)} with the same parameter returned <code>true</code>.
	 *
	 * @param object
	 *            The object to index. Must not be <code>null</code>.
	 * @return A {@link Set} of Doubles representing the indexed values for the given object. <code>null</code>-values in the set will be ignored. If this method returns <code>null</code>, it will be treated as if it had returned an empty set.
	 */
	@Override
	public Set<Double> getIndexValues(Object object);
}
