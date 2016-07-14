package org.chronos.chronodb.api.builder.query;

import java.util.regex.Pattern;

/**
 * This interface is a part of the fluent query API for ChronoDB.
 *
 * <p>
 * It allows to specify a variety of conditions in a "where" clause.
 *
 * <p>
 * Please see the individual methods for examples.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface WhereBuilder {

	/**
	 * Adds a text containment constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").contains("Martin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder contains(String text);

	/**
	 * Adds a case-insensitive text containment constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").containsIgnoreCase("martin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder containsIgnoreCase(String text);

	/**
	 * Adds a text not-containment constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notContains("Martin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notContains(String text);

	/**
	 * Adds a case-insensitive text not-containment constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notContainsIgnoreCase("martin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notContainsIgnoreCase(String text);

	/**
	 * Adds a text "starts with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").startsWith("Ma").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder startsWith(String text);

	/**
	 * Adds a case-insensitive text "starts with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").startsWithIgnoreCase("ma").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder startsWithIgnoreCase(String text);

	/**
	 * Adds a text "not starts with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notStartsWith("Ma").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notStartsWith(String text);

	/**
	 * Adds a case-insensitive text "not starts with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notStartsWithIgnoreCase("ma").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notStartsWithIgnoreCase(String text);

	/**
	 * Adds a text "ends with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").endsWith("rtin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder endsWith(String text);

	/**
	 * Adds a case-insensitive text "ends with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").endsWithIgnoreCase("rtin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder endsWithIgnoreCase(String text);

	/**
	 * Adds a text "not ends with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notEndsWith("rtin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notEndsWith(String text);

	/**
	 * Adds a case-insensitive text "not ends with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notEndsWithIgnoreCase("rtin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notEndsWithIgnoreCase(String text);

	/**
	 * Adds a regex constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").matchesRegex("He(ll)*o").getResult();
	 * </pre>
	 *
	 *
	 * @param regex
	 *            The regex to search for. Must not be <code>null</code> or empty. Supports the full range of
	 *            expressions defined in <code>java.util.regex.</code>{@link Pattern}.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder matchesRegex(String regex);

	/**
	 * Adds a negated regex constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notMatchesRegex("He(ll)*o").getResult();
	 * </pre>
	 *
	 *
	 * @param regex
	 *            The regex to search for. Must not be <code>null</code> or empty. Supports the full range of
	 *            expressions defined in <code>java.util.regex.</code>{@link Pattern}.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notMatchesRegex(String regex);

	/**
	 * Adds a text equality constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").isEqualTo("Martin").getResult();
	 * </pre>
	 *
	 *
	 * @param value
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isEqualTo(String value);

	/**
	 * Adds a case-insensitive text equality constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").isEqualToIgnoreCase("martin").getResult();
	 * </pre>
	 *
	 *
	 * @param value
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isEqualToIgnoreCase(String value);

	/**
	 * Adds a negated text equality constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").isNotEqualTo("Martin").getResult();
	 * </pre>
	 *
	 *
	 * @param value
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isNotEqualTo(String value);

	/**
	 * Adds a case-insensitive negated text equality constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").isNotEqualToIgnoreCase("Martin").getResult();
	 * </pre>
	 *
	 *
	 * @param value
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isNotEqualToIgnoreCase(String value);

}
