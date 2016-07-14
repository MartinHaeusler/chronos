package org.chronos.chronodb.api.query;

import static com.google.common.base.Preconditions.*;

import java.util.regex.Pattern;

import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.parser.ast.WhereElement;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

/**
 * A condition is a comparison operation to be applied inside a {@link WhereElement} in the query language.
 *
 * <p>
 * This class is primarily concerned with conditions on string comparisons.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public enum Condition {

	/** Standard equality condition. */
	EQUALS("=="),

	/** Inverted equality condition. */
	NOT_EQUALS("<>"),

	/** Checks if a string starts with a given character sequence. */
	STARTS_WITH("|-"),

	/** Inverted starts-with condition. */
	NOT_STARTS_WITH("!|-"),

	/** Checks if a string ends with a given character sequence. */
	ENDS_WITH("-|"),

	/** Inverted ends-with condition. */
	NOT_ENDS_WITH("!-|"),

	/** Checks if a string contains a given character sequence. */
	CONTAINS("contains"),

	/** Inverted contains condition. */
	NOT_CONTAINS("!contains"),

	/** Checks if a string matches a given {@linkplain java.util.regex.Pattern regular expression}. */
	MATCHES_REGEX("matches"),

	/** Inverted regex matching condition. */
	NOT_MATCHES_REGEX("!matches");

	/** The in-fix representation for this literal. */
	private String infix;

	/**
	 * Constructs a new literal, for internal use only.
	 *
	 * @param infixRepresentation
	 *            The in-fix representation of the condition. Must not be <code>null</code>.
	 */
	private Condition(final String infixRepresentation) {
		checkNotNull(infixRepresentation, "Precondition violation - argument 'infixRepresentation' must not be NULL!");
		this.infix = infixRepresentation;
	}

	/**
	 * Returns the in-fix representation of this condition.
	 *
	 * @return The in-fix representation. Never <code>null</code>.
	 */
	public String getInfix() {
		return this.infix;
	}

	/**
	 * Returns the negated form of this condition.
	 *
	 * <p>
	 * This also works for already negated conditions. The following statement is always true:
	 *
	 * <pre>
	 * condition.getNegated().getNegated() == condition // always true
	 * </pre>
	 *
	 * @return The negated condition. For already negated conditions, the regular non-negated condition will be
	 *         returned.
	 */
	public Condition getNegated() {
		switch (this) {
		case CONTAINS:
			return NOT_CONTAINS;
		case ENDS_WITH:
			return NOT_ENDS_WITH;
		case EQUALS:
			return NOT_EQUALS;
		case MATCHES_REGEX:
			return NOT_MATCHES_REGEX;
		case NOT_CONTAINS:
			return CONTAINS;
		case NOT_ENDS_WITH:
			return ENDS_WITH;
		case NOT_EQUALS:
			return EQUALS;
		case NOT_MATCHES_REGEX:
			return MATCHES_REGEX;
		case NOT_STARTS_WITH:
			return STARTS_WITH;
		case STARTS_WITH:
			return NOT_STARTS_WITH;
		default:
			throw new UnknownEnumLiteralException("Encountered unknown literal of Condition: '" + this + "'!");
		}
	}

	/**
	 * Checks if this condition is negated or not.
	 *
	 * @return <code>true</code> if this is a negated condition, otherwise <code>false</code>.
	 */
	public boolean isNegated() {
		switch (this) {
		case CONTAINS:
			return false;
		case ENDS_WITH:
			return false;
		case EQUALS:
			return false;
		case MATCHES_REGEX:
			return false;
		case NOT_CONTAINS:
			return true;
		case NOT_ENDS_WITH:
			return true;
		case NOT_EQUALS:
			return true;
		case NOT_MATCHES_REGEX:
			return true;
		case NOT_STARTS_WITH:
			return true;
		case STARTS_WITH:
			return false;
		default:
			throw new UnknownEnumLiteralException("Encountered unknown literal of Condition: '" + this + "'!");
		}
	}

	/**
	 * Applies <code>this</code> condition to the given text and search value, considering the given match mode.
	 *
	 * @param text
	 *            The text to search in. Must not be <code>null</code>.
	 * @param searchValue
	 *            The character sequence to search for. Must not be <code>null</code>.
	 * @param matchMode
	 *            The text match mode to use. Must not be <code>null</code>.
	 * @return <code>true</code> if this condition applies (matches) given the parameters, otherwise <code>false</code>
	 *         .
	 */
	public boolean applies(final String text, final String searchValue, final TextMatchMode matchMode) {
		switch (this) {
		case CONTAINS:
			switch (matchMode) {
			case CASE_INSENSITIVE:
				return text.toLowerCase().contains(searchValue.toLowerCase());
			case STRICT:
				return text.contains(searchValue);
			default:
				throw new UnknownEnumLiteralException(matchMode);
			}
		case ENDS_WITH:
			switch (matchMode) {
			case CASE_INSENSITIVE:
				return text.toLowerCase().endsWith(searchValue.toLowerCase());
			case STRICT:
				return text.endsWith(searchValue);
			default:
				throw new UnknownEnumLiteralException(matchMode);
			}
		case EQUALS:
			switch (matchMode) {
			case CASE_INSENSITIVE:
				return text.toLowerCase().equals(searchValue.toLowerCase());
			case STRICT:
				return text.equals(searchValue);
			default:
				throw new UnknownEnumLiteralException(matchMode);
			}
		case MATCHES_REGEX:
			switch (matchMode) {
			case CASE_INSENSITIVE:
				return Pattern.matches("(?i)" + searchValue, text);
			case STRICT:
				return Pattern.matches(searchValue, text);
			default:
				throw new UnknownEnumLiteralException(matchMode);
			}
		case STARTS_WITH:
			switch (matchMode) {
			case CASE_INSENSITIVE:
				return text.toLowerCase().startsWith(searchValue.toLowerCase());
			case STRICT:
				return text.startsWith(searchValue);
			default:
				throw new UnknownEnumLiteralException(matchMode);
			}
		case NOT_CONTAINS:
			return this.getNegated().applies(text, searchValue, matchMode) == false;
		case NOT_ENDS_WITH:
			return this.getNegated().applies(text, searchValue, matchMode) == false;
		case NOT_EQUALS:
			return this.getNegated().applies(text, searchValue, matchMode) == false;
		case NOT_MATCHES_REGEX:
			return this.getNegated().applies(text, searchValue, matchMode) == false;
		case NOT_STARTS_WITH:
			return this.getNegated().applies(text, searchValue, matchMode) == false;
		default:
			throw new UnknownEnumLiteralException(this);
		}
	}

}
