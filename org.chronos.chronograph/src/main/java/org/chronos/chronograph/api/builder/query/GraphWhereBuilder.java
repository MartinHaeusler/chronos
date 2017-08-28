package org.chronos.chronograph.api.builder.query;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.internal.impl.util.ChronoGraphQueryUtil;

/**
 * A step in the fluent graph query builder API. Specifies the details of a filer operation.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @param <E>
 *            Either {@link Vertex} or {@link Edge}, depending on the result type of this query.
 */
public interface GraphWhereBuilder<E extends Element> {

	// =================================================================================================================
	// STRING OPERATIONS
	// =================================================================================================================

	/**
	 * Specifies a "contains text" filter (case sensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> contains(String text);

	/**
	 * Specifies a "contains text" filter (case insensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> containsIgnoreCase(String text);

	/**
	 * Specifies a "not contains text" filter (case sensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> notContains(String text);

	/**
	 * Specifies a "not contains text" filter (case insensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> notContainsIgnoreCase(String text);

	/**
	 * Specifies a "starts with text" filter (case sensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> startsWith(String text);

	/**
	 * Specifies a "starts with text" filter (case insensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> startsWithIgnoreCase(String text);

	/**
	 * Specifies a "not starts with text" filter (case sensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> notStartsWith(String text);

	/**
	 * Specifies a "not starts with text" filter (case insensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> notStartsWithIgnoreCase(String text);

	/**
	 * Specifies a "ends with text" filter (case sensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> endsWith(String text);

	/**
	 * Specifies a "ends with text" filter (case insensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> endsWithIgnoreCase(String text);

	/**
	 * Specifies a "not ends with text" filter (case sensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> notEndsWith(String text);

	/**
	 * Specifies a "not ends with text" filter (case insensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> notEndsWithIgnoreCase(String text);

	/**
	 * Specifies a "matches regular expression" filter.
	 *
	 * @param regex
	 *            The regular expression to match. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> matchesRegex(String regex);

	/**
	 * Specifies a "not matches regular expression" filter.
	 *
	 * @param regex
	 *            The regular expression to match. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> notMatchesRegex(String regex);

	/**
	 * Specifies an "is equal to" filter (case sensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isEqualTo(String text);

	/**
	 * Specifies an "is equal to" filter (case insensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isEqualToIgnoreCase(String text);

	/**
	 * Specifies an "is not equal to" filter (case sensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isNotEqualTo(String text);

	/**
	 * Specifies an "is not equal to" filter (case insensitive).
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code>.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isNotEqualToIgnoreCase(String text);

	// =================================================================================================================
	// LONG OPERATIONS
	// =================================================================================================================

	/**
	 * Specifies an "is equal to" filter.
	 *
	 * @param value
	 *            The value to search for.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isEqualTo(long value);

	/**
	 * Specifies an "is not equal to" filter.
	 *
	 * @param value
	 *            The value to search for.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isNotEqualTo(long value);

	/**
	 * Specifies an "is (strictly) greater than" filter.
	 *
	 * @param value
	 *            The value to search for.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isGreaterThan(long value);

	/**
	 * Specifies an "is greater than or equal to" filter.
	 *
	 * @param value
	 *            The value to search for.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isGreaterThanOrEqualTo(long value);

	/**
	 * Specifies an "is (strictly) less than" filter.
	 *
	 * @param value
	 *            The value to search for.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isLessThan(long value);

	/**
	 * Specifies an "is less than or equal to" filter.
	 *
	 * @param value
	 *            The value to search for.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isLessThanOrEqualTo(long value);

	// =================================================================================================================
	// DOUBLE OPERATIONS
	// =================================================================================================================

	/**
	 * Specifies an "is equal to" filter.
	 *
	 * @param value
	 *            The value to search for.
	 *
	 * @param equalityTolerance
	 *            The tolerance interval for equality. Will be applied in positive and negative direction.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isEqualTo(double value, double equalityTolerance);

	/**
	 * Specifies an "is equal to" filter with a {@linkplain ChronoGraphQueryUtil#DOUBLE_EQUALITY_TOLERANCE default equality tolerance}.
	 *
	 * @param value
	 *            The value to search for.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public default GraphFinalizableQueryBuilder<E> isEqualTo(final double value) {
		return this.isEqualTo(value, ChronoGraphQueryUtil.DOUBLE_EQUALITY_TOLERANCE);
	}

	/**
	 * Specifies an "is not equal to" filter.
	 *
	 * @param value
	 *            The value to search for.
	 * @param equalityTolerance
	 *            The tolerance interval for equality. Will be applied in positive and negative direction.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isNotEqualTo(double value, double equalityTolerance);

	/**
	 * Specifies an "is not equal to" filter with a {@linkplain ChronoGraphQueryUtil#DOUBLE_EQUALITY_TOLERANCE default equality tolerance}.
	 *
	 * @param value
	 *            The value to search for.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public default GraphFinalizableQueryBuilder<E> isNotEqualTo(final double value) {
		return this.isNotEqualTo(value, ChronoGraphQueryUtil.DOUBLE_EQUALITY_TOLERANCE);
	}

	/**
	 * Specifies an "is (strictly) greater than" filter.
	 *
	 * @param value
	 *            The value to search for.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isGreaterThan(double value);

	/**
	 * Specifies an "is greater than or equal to" filter.
	 *
	 * @param value
	 *            The value to search for.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isGreaterThanOrEqualTo(double value);

	/**
	 * Specifies an "is (strictly) less than" filter.
	 *
	 * @param value
	 *            The value to search for.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isLessThan(double value);

	/**
	 * Specifies an "is less than or equal to" filter.
	 *
	 * @param value
	 *            The value to search for.
	 *
	 * @return The next builder in the fluent query API, for method chaining. Never <code>null</code>.
	 */
	public GraphFinalizableQueryBuilder<E> isLessThanOrEqualTo(double value);

}
