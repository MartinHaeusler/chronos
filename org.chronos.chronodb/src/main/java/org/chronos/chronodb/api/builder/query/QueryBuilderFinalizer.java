package org.chronos.chronodb.api.builder.query;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.chronos.chronodb.api.key.QualifiedKey;

import com.google.common.collect.Sets;

/**
 * Represents a final part in the query builder API, i.e. one that allows to actually execute the previously built
 * query.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface QueryBuilderFinalizer {

	/**
	 * Executes the previously built query.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * Iterator&lt;QualifiedKey&gt; iterator = tx.find().where("name").contains("hello").getKeys();
	 * </pre>
	 *
	 * @return An iterator over all keys that match the query. May be empty, but never <code>null</code>.
	 */
	public Iterator<QualifiedKey> getKeys();

	/**
	 * Executes the previously built query.
	 *
	 * <p>
	 * Please note that this method requires to completely exhaust the result iterator. For certain backends, this might
	 * be an expensive operation. It is recommended to use {@link #getKeys()} in favor of this method whenever possible,
	 * as it can lazily fetch the results on-demand.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * Set&lt;QualifiedKey&gt; resultSet = tx.find().where("name").contains("hello").getKeysAsSet();
	 * </pre>
	 *
	 * @return An immutable set of all keys that match the query. May be empty, but never <code>null</code>.
	 */
	public default Set<QualifiedKey> getKeysAsSet() {
		Iterator<QualifiedKey> iterator = this.getKeys();
		Set<QualifiedKey> resultSet = Sets.newHashSet();
		while (iterator.hasNext()) {
			QualifiedKey key = iterator.next();
			resultSet.add(key);
		}
		return Collections.unmodifiableSet(resultSet);
	}

	/**
	 * Executes the previously built query.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * Iterator&lt;Entry&lt;QualifiedKey, Object&gt;&gt; iterator = tx.find().where("name").contains("hello").getQualifiedResult();
	 * // now simply iterate over the result...
	 * while (iterator.hasNext()) {
	 * 	Entry&lt;QualifiedKey, Object&gt; entry = iterator.next();
	 * 	QualifiedKey qKey = entry.getKey();
	 * 	Object value = entry.getValue();
	 * 	// ... do something with them
	 * }
	 * </pre>
	 *
	 * @return An iterator over all qualified key-value pairs that match the query. May be empty, but never
	 *         <code>null</code>.
	 */
	public Iterator<Entry<QualifiedKey, Object>> getQualifiedResult();

	/**
	 * Executes the previously built query.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * Iterator&lt;Entry&lt;String, Object&gt;&gt; iterator = tx.find().where("name").contains("hello").getResult();
	 * // now simply iterate over the result...
	 * while (iterator.hasNext()) {
	 * 	Entry&lt;String, Object&gt; entry = iterator.next();
	 * 	String key = entry.getKey();
	 * 	Object value = entry.getValue();
	 * 	// ... do something with them
	 * }
	 * </pre>
	 *
	 * <p>
	 * <b>WARNING:</b><br>
	 * The result set of this method will contain <i>all</i> key-value combinations that match the query. If the query
	 * considers more than one keyspace, then the returned keys are not guaranteed to be unique. The reason is that a
	 * single key can simultaneously appear in multiple keyspaces with potentially different values. To disambiguate
	 * this situation, please consider using {@link #getQualifiedResult()} if you are dealing with queries across
	 * keyspaces.
	 *
	 * @return An iterator over all key-value pairs that match the query. May be empty, but never <code>null</code>.
	 *
	 * @see #getQualifiedResult()
	 */
	public Iterator<Entry<String, Object>> getResult();

	/**
	 * Executes the previously built query.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * Iterator&lt;Object&gt; iterator = tx.find().where("name").contains("hello").getValues();
	 * // now simply iterate over the result...
	 * while (iterator.hasNext()) {
	 * 	Object value = iterator.next();
	 * 	// ... do something with it
	 * }
	 * </pre>
	 *
	 * <p>
	 * <b>WARNING:</b> The returned iterator delivers the values for all key-value pairs that match the query. It is
	 * <b>not guaranteed</b> that any of the values will be unique!
	 *
	 * @return An iterator over all values that match the query. May be empty, but never <code>null</code>.
	 */
	public Iterator<Object> getValues();

	/**
	 * Executes the previously built query.
	 *
	 * <p>
	 * Please note that this method requires to completely exhaust the result iterator. For certain backends, this might
	 * be an expensive operation. It is recommended to use {@link #getValues()} in favor of this method whenever
	 * possible, as it can lazily fetch the results on-demand.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * Set&lt;Object&gt; resultSet = tx.find().where("name").contains("hello").getValuesAsSet();
	 * // now simply iterate over the result...
	 * for (Object result : resultSet) {
	 * 	// ... do something with it
	 * }
	 * </pre>
	 *
	 * @return An immutable set containing the objects returned by the query. May be empty, but never <code>null</code>.
	 */
	public default Set<Object> getValuesAsSet() {
		Iterator<Object> iterator = this.getValues();
		Set<Object> resultSet = Sets.newHashSet();
		while (iterator.hasNext()) {
			Object object = iterator.next();
			resultSet.add(object);
		}
		return Collections.unmodifiableSet(resultSet);
	}

}
