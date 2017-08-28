package org.chronos.chronodb.api.query;

import java.util.List;

import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.condition.string.ContainsCondition;
import org.chronos.chronodb.internal.impl.query.condition.string.EndsWithCondition;
import org.chronos.chronodb.internal.impl.query.condition.string.MatchesRegexCondition;
import org.chronos.chronodb.internal.impl.query.condition.string.NotContainsCondition;
import org.chronos.chronodb.internal.impl.query.condition.string.NotEndsWithCondition;
import org.chronos.chronodb.internal.impl.query.condition.string.NotMatchesRegexCondition;
import org.chronos.chronodb.internal.impl.query.condition.string.NotStartsWithCondition;
import org.chronos.chronodb.internal.impl.query.condition.string.StartsWithCondition;

/**
 * A {@link StringCondition} is a {@link Condition} on string values.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface StringCondition extends Condition {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	/** Checks if a string starts with a given character sequence. */
	public static final StringCondition STARTS_WITH = StartsWithCondition.INSTANCE;

	/** Inverted starts-with condition. */
	public static final StringCondition NOT_STARTS_WITH = NotStartsWithCondition.INSTANCE;

	/** Checks if a string ends with a given character sequence. */
	public static final StringCondition ENDS_WITH = EndsWithCondition.INSTANCE;

	/** Inverted ends-with condition. */
	public static final StringCondition NOT_ENDS_WITH = NotEndsWithCondition.INSTANCE;

	/** Checks if a string contains a given character sequence. */
	public static final StringCondition CONTAINS = ContainsCondition.INSTANCE;

	/** Inverted contains condition. */
	public static final StringCondition NOT_CONTAINS = NotContainsCondition.INSTANCE;

	/** Checks if a string matches a given {@linkplain java.util.regex.Pattern regular expression}. */
	public static final StringCondition MATCHES_REGEX = MatchesRegexCondition.INSTANCE;

	/** Inverted regex matching condition. */
	public static final StringCondition NOT_MATCHES_REGEX = NotMatchesRegexCondition.INSTANCE;

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	/**
	 * Returns a list of all known {@link StringCondition}s.
	 *
	 * @return The list of string conditions. Never <code>null</code>.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<StringCondition> values() {
		List conditions = Condition.values();
		List<StringCondition> resultList = conditions;
		resultList.add(STARTS_WITH);
		resultList.add(NOT_STARTS_WITH);
		resultList.add(ENDS_WITH);
		resultList.add(NOT_ENDS_WITH);
		resultList.add(CONTAINS);
		resultList.add(NOT_CONTAINS);
		resultList.add(MATCHES_REGEX);
		resultList.add(NOT_MATCHES_REGEX);
		return resultList;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public StringCondition negate();

	/**
	 * Applies <code>this</code> condition to the given text and search value, considering the given match mode.
	 *
	 * @param text
	 *            The text to search in. Must not be <code>null</code>.
	 * @param searchValue
	 *            The character sequence to search for. Must not be <code>null</code>.
	 * @param matchMode
	 *            The text match mode to use. Must not be <code>null</code>.
	 * @return <code>true</code> if this condition applies (matches) given the parameters, otherwise <code>false</code> .
	 */
	public boolean applies(final String text, final String searchValue, final TextMatchMode matchMode);
}
