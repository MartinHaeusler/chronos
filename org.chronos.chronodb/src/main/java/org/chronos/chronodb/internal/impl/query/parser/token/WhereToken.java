package org.chronos.chronodb.internal.impl.query.parser.token;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;

/**
 * A {@link WhereToken} specifies an atomic filter.
 *
 * <p>
 * It consists of three parts:
 * <ul>
 * <li><b>Index name:</b> The name of the index to query.
 * <li><b>Condition:</b> The condition to apply between the value in the index and the comparison value.
 * <li><b>Comparison value:</b> The value to match against.
 * </ul>
 *
 * For example, let's assume there is an index called "name". A WhereToken could represent the following:
 *
 * <pre>
 *    WHERE("name" Condition.EQUALS "Martin")
 * </pre>
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class WhereToken implements QueryToken {

	private String indexName;

	private Condition condition;

	private TextMatchMode matchMode = TextMatchMode.STRICT;

	private String comparisonValue;

	/**
	 * Constructs an empty {@link WhereToken}.
	 *
	 * <p>
	 * Please note that the resulting instance is not valid until all setter methods have been invoked on it.
	 */
	public WhereToken() {
	}

	/**
	 * Constructs a full {@link WhereToken}.
	 *
	 * @param indexName
	 *            The name of the index to use. Must not be <code>null</code>.
	 * @param condition
	 *            The condition to apply. Must not be <code>null</code>.
	 * @param matchMode
	 *            The text match mode to use. Must not be <code>null</code>.
	 * @param comparisonValue
	 *            The value to compare the indexed values against by applying the condition. Must not be
	 *            <code>null</code>.
	 */
	public WhereToken(final String indexName, final Condition condition, final TextMatchMode matchMode,
			final String comparisonValue) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(condition, "Precondition violation - argument 'condition' must not be NULL!");
		checkNotNull(matchMode, "Precondition violation - argument 'matchMode' must not be NULL!");
		checkNotNull(comparisonValue, "Precondition violation - argument 'comparisonValue' must not be NULL!");
		this.indexName = indexName;
		this.condition = condition;
		this.matchMode = matchMode;
		this.comparisonValue = comparisonValue;
	}

	/**
	 * Returns the name of the index.
	 *
	 * @return The index name. Never <code>null</code>.
	 */
	public String getIndexName() {
		if (this.indexName == null) {
			throw new IllegalStateException("WhereToken#getIndexName() was called before #setIndexName() was called!");
		}
		return this.indexName;
	}

	/**
	 * Sets the index name.
	 *
	 * @param indexName
	 *            The name of the index. Must not be <code>null</code>.
	 */
	public void setIndexName(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		this.indexName = indexName;
	}

	/**
	 * Returns the condition to apply.
	 *
	 * @return The condition. Never <code>null</code>.
	 */
	public Condition getCondition() {
		if (this.condition == null) {
			throw new IllegalStateException("WhereToken#getCondition() was called before #setCondition() was called!");
		}
		return this.condition;
	}

	/**
	 * Sets the condition to apply.
	 *
	 * @param condition
	 *            The condition to apply. Must not be <code>null</code>.
	 */
	public void setCondition(final Condition condition) {
		checkNotNull(condition, "Precondition violation - argument 'condition' must not be NULL!");
		this.condition = condition;
	}

	/**
	 * Returns the match mode.
	 *
	 * @return The match mode. Never <code>null</code>.
	 */
	public TextMatchMode getMatchMode() {
		return this.matchMode;
	}

	/**
	 * Sets the match mode to use.
	 *
	 * @param matchMode
	 *            The match mode to use. Must not be <code>null</code>.
	 */
	public void setMatchMode(final TextMatchMode matchMode) {
		checkNotNull(matchMode, "Precondition violation - argument 'matchMode' must not be NULL!");
		this.matchMode = matchMode;
	}

	/**
	 * Returns the comparison value.
	 *
	 * @return The comparison value. Never <code>null</code>.
	 */
	public String getComparisonValue() {
		if (this.comparisonValue == null) {
			throw new IllegalStateException(
					"WhereToken#getComparisonValue() was called before #setComparisonValue() was called!");
		}
		return this.comparisonValue;
	}

	/**
	 * Sets the comparison value.
	 *
	 * @param comparisonValue
	 *            The comparison value to use. Must not be <code>null</code>.
	 */
	public void setComparisonValue(final String comparisonValue) {
		checkNotNull(comparisonValue, "Precondition violation - argument 'comparisonValue' must not be NULL!");
		this.comparisonValue = comparisonValue;
	}

}
