package org.chronos.chronosphere.api.query;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface QueryStepBuilder<S, E> extends Iterator<E> {

	// =================================================================================================================
	// GENERIC QUERY METHODS
	// =================================================================================================================

	/**
	 * Applies the given filter predicate to all elements in the current result.
	 *
	 * @param predicate
	 *            The predicate to apply. Must not be <code>null</code>. All elements for which the predicate function
	 *            returns <code>false</code> will be filtered out and discarded.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public QueryStepBuilder<S, E> filter(Predicate<E> predicate);

	/**
	 * Limits the number of elements in the result set to the given number.
	 *
	 * <p>
	 * Please note that the order of elements is in general arbitrary. Therefore, usually a {@link #limit(long)} is
	 * preceded by a {@link #orderBy(Comparator)}.
	 *
	 * @param limit
	 *            The limit to apply. Must not be negative.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public QueryStepBuilder<S, E> limit(long limit);

	/**
	 * Sorts the current result set by applying the given comparator.
	 *
	 * <p>
	 * This method requires full resolution of all elements and is therefore <b>non-lazy</b>.
	 *
	 * @param comparator
	 *            The comparator to use. Must not be <code>null</code>.
	 *
	 * @return The query builder, for method chaining. Never <code>null</code>.
	 */
	public QueryStepBuilder<S, E> orderBy(Comparator<E> comparator);

	/**
	 * Eliminates all duplicated values from the current result.
	 *
	 * <p>
	 * This method is lazy, but requires to keep track of the "already encountered" elements in a set. Therefore, RAM
	 * consumption on this method may be high if it is applied on very large result sets.
	 *
	 * @return The distinct step builder, for method chaining. Never <code>null</code>.
	 */
	public QueryStepBuilder<S, E> distinct();

	/**
	 * Uses the given function to map each element from the current result set to a new element.
	 *
	 * @param function
	 *            The mapping function to apply. Must not be <code>null</code>. Should be idempotent and side-effect
	 *            free.
	 *
	 * @return The query step builder, for method chaining. Never <code>null</code>.
	 */
	public <T> UntypedQueryStepBuilder<S, T> map(Function<E, T> function);

	/**
	 * Uses the given function to map each element to an iterator of output elements. These output iterators will be
	 * concatenated and forwarded.
	 *
	 * @param function
	 *            The map function to apply. Must not be <code>null</code>. Should be idempotent and side-effect free.
	 * @return The query step builder, for method chaining. Never <code>null</code>.
	 */
	public <T> UntypedQueryStepBuilder<S, T> flatMap(Function<E, Iterator<T>> function);

	/**
	 * Filters out and discards all <code>null</code> values from the current result set.
	 *
	 * <p>
	 * This is the same as:
	 *
	 * <pre>
	 * query.filter(element -> element != null)
	 * </pre>
	 *
	 * @return The query step builder, for method chaining. Never <code>null</code>.
	 *
	 * @see #filter(Predicate)
	 */
	public QueryStepBuilder<S, E> notNull();

	// =================================================================================================================
	// NAMED OPERATIONS
	// =================================================================================================================

	/**
	 * Assigns the given name to this {@link QueryStepBuilder}.
	 *
	 * <p>
	 * Note that only the <i>step</i> is named. When coming back to this step, the query result may be different than it
	 * was when it was first reached, depending on the traversal.
	 *
	 * @param stepName
	 *            The name of the step. Must not be <code>null</code>. Must be unique within the query.
	 *
	 * @return The named step, for method chaining. Never <code>null</code>.
	 *
	 * @see #back(String)
	 */
	public QueryStepBuilder<S, E> named(String stepName);

	/**
	 * Exits the current traversal state and goes back to a named step, or a named set.
	 *
	 * @param stepName
	 *            The name of the step to go back to. Must not be <code>null</code>, must refer to a named step.
	 *
	 * @return The step after going back, for method chaining. Never <code>null</code>.
	 *
	 * @see #named(String)
	 */
	public UntypedQueryStepBuilder<S, Object> back(String stepName);

	/**
	 * Removes all elements from the given {@linkplain #named(String) named step} from this step.
	 *
	 * @param stepName
	 *            The name of the step. Must not be <code>null</code>, must refer to a named step.
	 *
	 * @return The step after removing the elements in the given named step, for method chaining. Never
	 *         <code>null</code>.
	 */
	public QueryStepBuilder<S, E> except(String stepName);

	/**
	 * Removes all elements in the given set from the stream.
	 *
	 * @param elementsToExclude
	 *            The elements to remove. Must not be <code>null</code>.
	 * @return The step after removing the elements in the given set, for method chaining. Never <code>null</code>.
	 */
	public QueryStepBuilder<S, E> except(Set<?> elementsToExclude);

	// =====================================================================================================================
	// SET & BOOLEAN OPERATIONS
	// =====================================================================================================================

	@SuppressWarnings("unchecked")
	public UntypedQueryStepBuilder<S, Object> union(QueryStepBuilder<E, ?>... subqueries);

	@SuppressWarnings("unchecked")
	public QueryStepBuilder<S, E> and(QueryStepBuilder<E, ?>... subqueries);

	@SuppressWarnings("unchecked")
	public QueryStepBuilder<S, E> or(QueryStepBuilder<E, ?>... subqueries);

	public QueryStepBuilder<S, E> not(QueryStepBuilder<E, ?> subquery);

	// =================================================================================================================
	// FINISHING OPERATIONS
	// =================================================================================================================

	/**
	 * Calculates the result set of this query and returns it.
	 *
	 * <p>
	 * Please note that iterating over the result set via {@link #hasNext()} and {@link #next()} is in general faster
	 * and consumes less RAM than first converting the result into a set.
	 *
	 * @return The result set. Never <code>null</code>. May be empty.
	 */
	public Set<E> toSet();

	/**
	 * Calculates the result of this query and returns it as a {@link List}.
	 *
	 * <p>
	 * Please note that iterating over the result via {@link #hasNext()} and {@link #next()} is in general faster and
	 * consumes less RAM than first converting the result into a list.
	 *
	 * @return The result list. Never <code>null</code>. May be empty.
	 */
	public List<E> toList();

	/**
	 * Converts this query into a {@link Stream}.
	 *
	 * @return The stream representation of this query.
	 */
	public Stream<E> toStream();

	/**
	 * Counts the number of elements in the current result, and returns that count.
	 *
	 * @return The count. Never negative, may be zero.
	 */
	public long count();

}