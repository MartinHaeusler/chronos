package org.chronos.chronograph.internal.impl.util;

import static com.google.common.base.Preconditions.*;

import java.util.Set;

import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.query.SearchSpecification;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;

import com.google.common.collect.Sets;

public class ChronoGraphQueryUtil {

	/**
	 * Disassembles the given {@link SearchSpecification} into its individual parts and runs
	 * {@link #conditionApplies(Condition, Object, String, TextMatchMode)}.
	 *
	 * @param searchSpec
	 *            The search spec to test on the given value. Must not be <code>null</code>.
	 * @param value
	 *            The value to test. May be <code>null</code>.
	 * @return <code>true</code> if the given search specification applies to the given value, otherwise
	 *         <code>false</code>.
	 */
	public static boolean searchSpecApplies(final SearchSpecification searchSpec, final Object value) {
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		return conditionApplies(searchSpec.getCondition(), value, searchSpec.getSearchText(),
				searchSpec.getMatchMode());
	}

	/**
	 * Checks if the given {@link Condition} applies to the given value, using the provided search text and match mode.
	 *
	 * <p>
	 * This method has the following semantics:
	 * <ul>
	 * <li>If the <code>value</code> is not an instance of {@link Iterable}, it will be converted with
	 * {@link Object#toString()}, and the condition will be {@link Condition#applies(String, String, TextMatchMode)
	 * applied} to the resulting string. The result of that operation will be returned.
	 * <li>If the <code>value</code> is an instance of {@link Iterable}, its values will be put into a {@link Set}. Each
	 * element of the set will be converted with {@link Object#toString()}, and the condition will be
	 * {@link Condition#applies(String, String, TextMatchMode) applied} to the resulting string. This method returns
	 * <code>true</code> if any element in the set matches the condition, or <code>false</code> if no element in the set
	 * matches the condition.
	 * </ul>
	 *
	 * <p>
	 * Examples (brackets represent iterable values):
	 * <ul>
	 * <li>Condition STARTS_WITH "abc" on value "abcdefg" -> result will be <code>true</code> (match on the object
	 * itself).
	 * <li>Condition STARTS_WITH "abc" on value ["def", "abcde"] -> result will be <code>true</code> (match on "abcde").
	 * <li>Condition STARTS_WITH "abc" on value ["def", "ghi"] -> result will be <code>false</code> (no element
	 * matched).
	 * </ul>
	 *
	 * @param condition
	 *            The condition to test. Must not be <code>null</code>.
	 * @param value
	 *            The value to test. May be <code>null</code>. Will be tested after calling {@link Object#toString()} on
	 *            it. If it is an instance of {@link Iterable}, the condition will be tested on all elements.
	 * @param searchText
	 *            The text to search for. Must not be <code>null</code>.
	 * @param matchMode
	 *            The match mode to apply. Must not be <code>null</code>.
	 * @return <code>true</code> if the given condition applies to the given value, otherwise <code>false</code>.
	 */
	public static boolean conditionApplies(final Condition condition, final Object value, final String searchText,
			final TextMatchMode matchMode) {
		checkNotNull(condition, "Precondition violation - argument 'condition' must not be NULL!");
		checkNotNull(searchText, "Precondition violation - argument 'searchText' must not be NULL!");
		checkNotNull(matchMode, "Precondition violation - argument 'matchMode' must not be NULL!");
		// extract the set of strings from the object to match against
		Set<String> values = extractValues(value);
		// iterate over the strings; if the condition matches on any element, we consider it to match the whole value
		for (String element : values) {
			if (condition.applies(element, searchText, matchMode)) {
				// we found one match -> the entire value matches
				return true;
			}
		}
		// no element in our collection did match -> the value did not match
		return false;
	}

	private static Set<String> extractValues(final Object value) {
		Set<String> resultSet = Sets.newHashSet();
		if (value == null) {
			return resultSet;
		}
		if (value instanceof Iterable == false) {
			// just use the string reperesentation of the value
			resultSet.add(value.toString());
			return resultSet;
		}
		// iterate over the values and put everything in a collection
		@SuppressWarnings("unchecked")
		Iterable<? extends Object> iterable = (Iterable<? extends Object>) value;
		for (Object element : iterable) {
			resultSet.add(element.toString());
		}
		return resultSet;
	}
}
