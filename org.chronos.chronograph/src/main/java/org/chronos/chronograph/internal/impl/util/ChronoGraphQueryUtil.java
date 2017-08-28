package org.chronos.chronograph.internal.impl.util;

import static com.google.common.base.Preconditions.*;

import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.api.query.searchspec.DoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.LongSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.util.ReflectionUtils;

import com.google.common.collect.Sets;

public class ChronoGraphQueryUtil {

	/** The default equality tolerance in graph queries that operate on <code>double</code> values (=10e-6). */
	public static final Double DOUBLE_EQUALITY_TOLERANCE = 10e-6;

	/**
	 * Attempts to match the given value against the given search specification.
	 *
	 * <p>
	 * If the value is an {@link Iterable}, each value will be checked individually, and this method will return <code>true</code> if the search specification applies for at least one element of the iterable.
	 *
	 * @param searchSpec
	 *            The search spec to test on the given value. Must not be <code>null</code>.
	 * @param value
	 *            The value to test. May be <code>null</code>. * @param numberIndexingMode The indexing mode to apply. See documentation of enum literals for details. Must not be <code>null</code>.
	 * @return <code>true</code> if the given search specification applies to the given value, otherwise <code>false</code>.
	 */
	public static boolean searchSpecApplies(final SearchSpecification<?> searchSpec, final Object value) {
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		if (searchSpec instanceof StringSearchSpecification) {
			return searchSpecApplies((StringSearchSpecification) searchSpec, value);
		} else if (searchSpec instanceof LongSearchSpecification) {
			return searchSpecApplies((LongSearchSpecification) searchSpec, value);
		} else if (searchSpec instanceof DoubleSearchSpecification) {
			return searchSpecApplies((DoubleSearchSpecification) searchSpec, value);
		} else {
			throw new IllegalStateException("Unknown SearchSpecification class: '" + searchSpec.getClass().getName() + "'!");
		}
	}

	/**
	 * Attempts to match the given value against the given search specification.
	 *
	 * <p>
	 * If the value is an {@link Iterable}, each value will be checked individually, and this method will return <code>true</code> if the search specification applies for at least one element of the iterable.
	 *
	 * @param searchSpec
	 *            The search spec to test on the given value. Must not be <code>null</code>.
	 * @param value
	 *            The value to test. May be <code>null</code>.
	 * @return <code>true</code> if the given search specification applies to the given value, otherwise <code>false</code>.
	 */
	public static boolean searchSpecApplies(final StringSearchSpecification searchSpec, final Object value) {
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		if (value == null) {
			return false;
		}
		Set<Object> extractedValues = extractValues(value);
		for (Object element : extractedValues) {
			if (element instanceof String == false) {
				continue;
			}
			String stringVal = (String) element;
			if (searchSpec.matches(stringVal)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Attempts to match the given value against the given search specification.
	 *
	 * <p>
	 * If the value is an {@link Iterable}, each value will be checked individually, and this method will return <code>true</code> if the search specification applies for at least one element of the iterable.
	 *
	 * @param searchSpec
	 *            The search spec to test on the given value. Must not be <code>null</code>.
	 * @param value
	 *            The value to test. May be <code>null</code>.
	 * @return <code>true</code> if the given search specification applies to the given value, otherwise <code>false</code>.
	 */
	public static boolean searchSpecApplies(final LongSearchSpecification searchSpec, final Object value) {
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		if (value == null) {
			return false;
		}
		Set<Object> extractedValues = extractValues(value);
		for (Object element : extractedValues) {
			if (ReflectionUtils.isLongCompatible(element) == false) {
				continue;
			}
			long longVal = ReflectionUtils.asLong(value);
			if (searchSpec.matches(longVal)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Attempts to match the given value against the given search specification.
	 *
	 * <p>
	 * If the value is an {@link Iterable}, each value will be checked individually, and this method will return <code>true</code> if the search specification applies for at least one element of the iterable.
	 *
	 * @param searchSpec
	 *            The search spec to test on the given value. Must not be <code>null</code>.
	 * @param value
	 *            The value to test. May be <code>null</code>.
	 *
	 * @return <code>true</code> if the given search specification applies to the given value, otherwise <code>false</code>.
	 */
	public static boolean searchSpecApplies(final DoubleSearchSpecification searchSpec, final Object value) {
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		if (value == null) {
			return false;
		}
		Set<Object> extractedValues = extractValues(value);
		for (Object element : extractedValues) {
			if (ReflectionUtils.isDoubleCompatible(element) == false) {
				continue;
			}
			double doubleVal = ReflectionUtils.asDouble(value);
			if (searchSpec.matches(doubleVal)) {
				return true;
			}
		}
		return false;
	}

	private static Set<Object> extractValues(final Object value) {
		Set<Object> resultSet = Sets.newHashSet();
		if (value == null) {
			return resultSet;
		}
		if (value instanceof Iterable == false) {
			// just use the string reperesentation of the value
			resultSet.add(value);
			return resultSet;
		}
		// iterate over the values and put everything in a collection
		@SuppressWarnings("unchecked")
		Iterable<? extends Object> iterable = (Iterable<? extends Object>) value;
		for (Object element : iterable) {
			resultSet.add(element);
		}
		return resultSet;
	}

	public static NumberCondition gremlinCompareToNumberCondition(final Compare compare) {
		switch (compare) {
		case eq:
			return NumberCondition.EQUALS;
		case neq:
			return NumberCondition.NOT_EQUALS;
		case gt:
			return NumberCondition.GREATER_THAN;
		case gte:
			return NumberCondition.GREATER_EQUAL;
		case lt:
			return NumberCondition.LESS_THAN;
		case lte:
			return NumberCondition.LESS_EQUAL;
		default:
			throw new UnknownEnumLiteralException(compare);
		}
	}
}
