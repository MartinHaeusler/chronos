package org.chronos.chronodb.internal.impl.query.parser.token;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.api.query.StringCondition;
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
 * Structurally, the token consists of an {@link #indexName} and a {@link #whereDetails details} object. The details object varies by the kind of index that is being queried (e.g. {@link StringWhereDetails} for string search, {@link LongWhereDetails} for long search...).
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class WhereToken implements QueryToken {

	/** The name of the index to query when evaluating this token. Must not be <code>null</code>. */
	private String indexName;
	/** The details object assigned to this token. */
	private AbstractWhereDetails<?, ?> whereDetails;

	/**
	 * Constructs an empty {@link WhereToken}.
	 *
	 * <p>
	 * Please note that the resulting instance is not valid until all setter methods have been invoked on it.
	 */
	public WhereToken() {
	}

	/**
	 * Constructs a new {@link WhereToken} and calls {@link #setStringWhereDetails(StringCondition, TextMatchMode, String)} on it.
	 *
	 * @param index
	 *            The name of the index to refer in this "where" token. Must not be <code>null</code>.
	 * @param condition
	 *            The condition to apply in this "where" token. Must not be <code>null</code>.
	 * @param matchMode
	 *            The text match mode to use in this "where" token. See enum literal documentation for details. Must not be <code>null</code>.
	 * @param searchString
	 *            The search string to assign to this "where" token. Must not be <code>null</code>.
	 */
	public WhereToken(final String index, final StringCondition condition, final TextMatchMode matchMode, final String searchString) {
		this();
		this.setIndexName(index);
		this.setStringWhereDetails(condition, matchMode, searchString);
	}

	/**
	 * Constructs a new {@link WhereToken} and calls {@link #setLongWhereDetails(NumberCondition, long)} on it.
	 *
	 * @param index
	 *            The name of the index to refer in this "where" token. Must not be <code>null</code>.
	 * @param condition
	 *            The condition to apply in this "where" token. Must not be <code>null</code>.
	 * @param searchValue
	 *            The search value to store in this "where" token.
	 */
	public WhereToken(final String index, final NumberCondition condition, final long searchValue) {
		this();
		this.setIndexName(index);
		this.setLongWhereDetails(condition, searchValue);
	}

	/**
	 * Constructs a new {@link WhereToken} and calls {@link #setLongWhereDetails(NumberCondition, long)} on it.
	 *
	 * @param index
	 *            The name of the index to refer in this "where" token. Must not be <code>null</code>.
	 * @param condition
	 *            The condition to apply in this "where" token. Must not be <code>null</code>.
	 * @param searchValue
	 *            The search value to store in this "where" token.
	 * @param equalityTolerance
	 *            The equality tolerance to store in this "where" token. It is the maximum absolute difference between two doubles that are still considered to be "equal". Must not be negative.
	 */
	public WhereToken(final String index, final NumberCondition condition, final double searchValue, final double equalityTolerance) {
		this();
		this.setIndexName(index);
		this.setDoubleDetails(condition, searchValue, equalityTolerance);
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
	 * Sets the {@link StringWhereDetails} on this token.
	 *
	 * <p>
	 * Please note that any {@link WhereToken} can have at most one details object assigned at any point in time. If there already is a details object of a different type attached, this method will throw an {@link IllegalStateException}. If there is a details object of the same type attached, its values will be overwritten.
	 *
	 * @param condition
	 *            The condition to store in this "where" token. Must not be <code>null</code>.
	 * @param matchMode
	 *            The text match mode to store in this "where" token. Must not be <code>null</code>.
	 * @param comparisonValue
	 *            The comparison value to store in this "where" token. Must not be <code>null</code>.
	 */
	public void setStringWhereDetails(final StringCondition condition, final TextMatchMode matchMode,
			final String comparisonValue) {
		checkNotNull(condition, "Precondition violation - argument 'condition' must not be NULL!");
		checkNotNull(matchMode, "Precondition violation - argument 'matchmode' must not be NULL!");
		checkNotNull(comparisonValue, "Precondition violation - argument 'comparisonValue' must not be NULL!");
		if (this.whereDetails != null && this.whereDetails instanceof StringWhereDetails == false) {
			throw new IllegalStateException("Attempted to set string search details on a Where token that has search details of a different type assigned!");
		}
		if (this.whereDetails == null) {
			this.whereDetails = new StringWhereDetails(condition, matchMode, comparisonValue);
		} else {
			StringWhereDetails stringWhereDetails = (StringWhereDetails) this.whereDetails;
			stringWhereDetails.setCondition(condition);
			stringWhereDetails.setMatchMode(matchMode);
			stringWhereDetails.setComparisonValue(comparisonValue);
		}
	}

	/**
	 * Sets the {@link LongWhereDetails} on this token.
	 *
	 * <p>
	 * Please note that any {@link WhereToken} can have at most one details object assigned at any point in time. If there already is a details object of a different type attached, this method will throw an {@link IllegalStateException}. If there is a details object of the same type attached, its values will be overwritten.
	 *
	 * @param condition
	 *            The condition to store in this "where" token. Must not be <code>null</code>.
	 * @param comparisonValue
	 *            The comparison value to store in this "where" token.
	 */
	public void setLongWhereDetails(final NumberCondition condition, final long comparisonValue) {
		checkNotNull(condition, "Precondition violation - argument 'condition' must not be NULL!");
		if (this.whereDetails != null && this.whereDetails instanceof LongWhereDetails == false) {
			throw new IllegalStateException("Attempted to set long search details on a Where token that has search details of a different type assigned!");
		}
		if (this.whereDetails == null) {
			this.whereDetails = new LongWhereDetails(condition, comparisonValue);
		} else {
			LongWhereDetails longWhereDetails = (LongWhereDetails) this.whereDetails;
			longWhereDetails.setCondition(condition);
			longWhereDetails.setComparisonValue(comparisonValue);
		}
	}

	/**
	 * Sets the {@link DoubleWhereDetails} on this token.
	 *
	 * <p>
	 * Please note that any {@link WhereToken} can have at most one details object assigned at any point in time. If there already is a details object of a different type attached, this method will throw an {@link IllegalStateException}. If there is a details object of the same type attached, its values will be overwritten.
	 *
	 * @param condition
	 *            The condition to store in this "where" token. Must not be <code>null</code>.
	 * @param comparisonValue
	 *            The comparison value to store in this "where" token.
	 * @param equalityTolerance
	 *            The equality tolerance to store in this "where" token. Must not be negative.
	 */
	public void setDoubleDetails(final NumberCondition condition, final double comparisonValue, final double equalityTolerance) {
		checkNotNull(condition, "Precondition violation - argument 'condition' must not be NULL!");
		checkArgument(equalityTolerance >= 0, "Precondition violation - argument 'equalityTolerance' must not be negative!");
		if (this.whereDetails != null && this.whereDetails instanceof DoubleWhereDetails == false) {
			throw new IllegalStateException("Attempted to set double search details on a Where token that has search details of a different type assigned!");
		}
		if (this.whereDetails == null) {
			this.whereDetails = new DoubleWhereDetails(condition, comparisonValue, equalityTolerance);
		} else {
			DoubleWhereDetails doubleWhereDetails = (DoubleWhereDetails) this.whereDetails;
			doubleWhereDetails.setCondition(condition);
			doubleWhereDetails.setComparisonValue(comparisonValue);
			doubleWhereDetails.setEqualityTolerance(equalityTolerance);
		}
	}

	/**
	 * Returns the {@link StringWhereDetails} attached to this element.
	 *
	 * <p>
	 * Please note that a {@link WhereToken} can have at most one "details" object attached. If there is no details object attached, or a details object of a different type is attached, then this method will throw an {@link IllegalStateException}. Use {@link #isStringWhereToken()} first to check if calling this method is safe or not.
	 *
	 * @return The details object. Never <code>null</code>.
	 *
	 * @throws IllegalStateException
	 *             Thrown if no details object is present or it is of the wrong type.
	 */
	public StringWhereDetails asStringWhereToken() {
		if (this.whereDetails == null) {
			throw new IllegalStateException("Requested the Where details for String search, but no details have been assigned yet!");
		}
		if (this.whereDetails instanceof StringWhereDetails == false) {
			throw new IllegalStateException("Requested the Where details for String search, but details of a different type were assigned!");
		}
		return (StringWhereDetails) this.whereDetails;
	}

	/**
	 * Returns the {@link LongWhereDetails} attached to this element.
	 *
	 * <p>
	 * Please note that a {@link WhereToken} can have at most one "details" object attached. If there is no details object attached, or a details object of a different type is attached, then this method will throw an {@link IllegalStateException}. Use {@link #isLongWhereToken()} first to check if calling this method is safe or not.
	 *
	 * @return The details object. Never <code>null</code>.
	 *
	 * @throws IllegalStateException
	 *             Thrown if no details object is present or it is of the wrong type.
	 */
	public LongWhereDetails asLongWhereToken() {
		if (this.whereDetails == null) {
			throw new IllegalStateException("Requested the Where details for Long search, but no details have been assigned yet!");
		}
		if (this.whereDetails instanceof LongWhereDetails == false) {
			throw new IllegalStateException("Requested the Where details for Long search, but details of a different type were assigned!");
		}
		return (LongWhereDetails) this.whereDetails;
	}

	/**
	 * Returns the {@link DoubleWhereDetails} attached to this element.
	 *
	 * <p>
	 * Please note that a {@link WhereToken} can have at most one "details" object attached. If there is no details object attached, or a details object of a different type is attached, then this method will throw an {@link IllegalStateException}. Use {@link #isDoubleWhereToken()} first to check if calling this method is safe or not.
	 *
	 * @return The details object. Never <code>null</code>.
	 *
	 * @throws IllegalStateException
	 *             Thrown if no details object is present or it is of the wrong type.
	 */
	public DoubleWhereDetails asDoubleWhereToken() {
		if (this.whereDetails == null) {
			throw new IllegalStateException("Requested the Where details for Double search, but no details have been assigned yet!");
		}
		if (this.whereDetails instanceof DoubleWhereDetails == false) {
			throw new IllegalStateException("Requested the Where details for Long search, but details of a different type were assigned!");
		}
		return (DoubleWhereDetails) this.whereDetails;
	}

	/**
	 * Checks if this element has {@link StringWhereDetails} assigned to it.
	 *
	 * @return <code>true</code> if StringWhereDetails are assigned, otherwise <code>false</code>.
	 */
	public boolean isStringWhereToken() {
		return this.whereDetails instanceof StringWhereDetails;
	}

	/**
	 * Checks if this element has {@link LongWhereDetails} assigned to it.
	 *
	 * @return <code>true</code> if LongWhereDetails are assigned, otherwise <code>false</code>.
	 */
	public boolean isLongWhereToken() {
		return this.whereDetails instanceof LongWhereDetails;
	}

	/**
	 * Checks if this element has {@link DoubleWhereDetails} assigned to it.
	 *
	 * @return <code>true</code> if DoubleWhereDetails are assigned, otherwise <code>false</code>.
	 */
	public boolean isDoubleWhereToken() {
		return this.whereDetails instanceof DoubleWhereDetails;
	}

	/**
	 * Checks if this element already has {@linkplain AbstractWhereDetails details} assigned to it.
	 *
	 * @return <code>true</code> if details are present, otherwise <code>false</code>.
	 */
	public boolean hasDetailsAssigned() {
		return this.whereDetails != null;
	}

	/**
	 * A simple data container, specifying the details of a {@link WhereToken} in the query language.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 * @param <VALUE>
	 *            The type of the comparison value to store.
	 * @param <CONDITION>
	 *            The type of the {@linkplain Condition comparison condition} to store.
	 */
	public abstract class AbstractWhereDetails<VALUE, CONDITION extends Condition> {

		/** The comparison condition to apply between the indexed values and the {@link #comparisonValue}. */
		protected CONDITION condition;
		/** The comparison value, i.e. the value that will be {@linkplain #condition compared} against the indexed values. */
		protected VALUE comparisonValue;

		/**
		 * Constructs a full {@link StringWhereDetails}.
		 *
		 * @param condition
		 *            The condition to apply. Must not be <code>null</code>.
		 * @param comparisonValue
		 *            The value to compare the indexed values against by applying the condition. Must not be <code>null</code>.
		 */
		protected AbstractWhereDetails(final CONDITION condition, final VALUE comparisonValue) {
			checkNotNull(condition, "Precondition violation - argument 'condition' must not be NULL!");
			checkNotNull(comparisonValue, "Precondition violation - argument 'comparisonValue' must not be NULL!");
			this.condition = condition;
			this.comparisonValue = comparisonValue;
		}

		/**
		 * Returns the condition to apply.
		 *
		 * @return The condition. Never <code>null</code>.
		 */
		public CONDITION getCondition() {
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
		public void setCondition(final CONDITION condition) {
			checkNotNull(condition, "Precondition violation - argument 'condition' must not be NULL!");
			this.condition = condition;
		}

		/**
		 * Returns the comparison value.
		 *
		 * @return The comparison value. Never <code>null</code>.
		 */
		public VALUE getComparisonValue() {
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
		public void setComparisonValue(final VALUE comparisonValue) {
			checkNotNull(comparisonValue, "Precondition violation - argument 'comparisonValue' must not be NULL!");
			this.comparisonValue = comparisonValue;
		}

		/**
		 * Returns the name of the index to scan.
		 *
		 * @return The index name. Never <code>null</code>.
		 */
		public String getIndexName() {
			return WhereToken.this.getIndexName();
		}

		/**
		 * Sets the index name.
		 *
		 * @param indexName
		 *            The name of the index. Must not be <code>null</code>.
		 */
		public void setIndexName(final String indexName) {
			WhereToken.this.setIndexName(indexName);
		}

	}

	/**
	 * A simple data container, specifying the details of a {@link WhereToken} in the query language.
	 *
	 * <p>
	 * This version is specialized for "where" clauses that act on {@link String} values.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 */
	public class StringWhereDetails extends AbstractWhereDetails<String, StringCondition> {

		/** The match mode to apply when comparing two strings. See enum literals for details. */
		private TextMatchMode matchMode = TextMatchMode.STRICT;

		/**
		 * Constructs a full {@link StringWhereDetails}.
		 *
		 * @param condition
		 *            The condition to apply. Must not be <code>null</code>.
		 * @param matchMode
		 *            The text match mode to use. Must not be <code>null</code>.
		 * @param comparisonValue
		 *            The value to compare the indexed values against by applying the condition. Must not be <code>null</code>.
		 */
		public StringWhereDetails(final StringCondition condition, final TextMatchMode matchMode,
				final String comparisonValue) {
			super(condition, comparisonValue);
			checkNotNull(matchMode, "Precondition violation - argument 'matchMode' must not be NULL!");
			this.matchMode = matchMode;
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

	}

	/**
	 * A simple data container, specifying the details of a {@link WhereToken} in the query language.
	 *
	 * <p>
	 * This version is specialized for "where" clauses that act on {@link Long} values.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 */
	public class LongWhereDetails extends AbstractWhereDetails<Long, NumberCondition> {

		/**
		 * Creates a new {@link LongWhereDetails} instance.
		 *
		 * @param condition
		 *            The condition to store in the new details object. Must not be <code>null</code>.
		 * @param comparisonValue
		 *            The comparison value to store in the new details object.
		 */
		protected LongWhereDetails(final NumberCondition condition, final long comparisonValue) {
			super(condition, comparisonValue);
		}

	}

	/**
	 * A simple data container, specifying the details of a {@link WhereToken} in the query language.
	 *
	 * <p>
	 * This version is specialized for "where" clauses that act on {@link Double} values.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 */
	public class DoubleWhereDetails extends AbstractWhereDetails<Double, NumberCondition> {

		/** The equality tolerance, i.e. the absolute value by which two doubles may differ but still be considered equal. */
		private double equalityTolerance;

		/**
		 * Creates a new {@link DoubleWhereDetails} instance.
		 *
		 * @param condition
		 *            The condition to store in the new details object. Must not be <code>null</code>.
		 * @param comparisonValue
		 *            The comparison value to store in the new details object.
		 * @param equalityTolerance
		 *            The equality tolerance to store in the new details object. Must not be negative. For details, see {@link #setEqualityTolerance(double)}.
		 */
		protected DoubleWhereDetails(final NumberCondition condition, final double comparisonValue, final double equalityTolerance) {
			super(condition, comparisonValue);
			checkArgument(equalityTolerance >= 0, "Precondition violation - argument 'equalityTolerance' must not be negative!");
			this.equalityTolerance = equalityTolerance;
		}

		/**
		 * Returns the assigned equality tolerance.
		 *
		 * <p>
		 * The equality tolerance is the (absolute) value by which two double values can differ and still be considered equal.
		 *
		 * @return The equality tolerance. Never negative.
		 */
		public double getEqualityTolerance() {
			return this.equalityTolerance;
		}

		/**
		 * Sets the equality tolerance.
		 *
		 * <p>
		 * The equality tolerance is the (absolute) value by which two double values can differ and still be considered equal.
		 *
		 *
		 * @param equalityTolerance
		 *            The equality tolerance to use. Must not be negative.
		 */
		public void setEqualityTolerance(final double equalityTolerance) {
			checkArgument(equalityTolerance >= 0, "Precondition violation - argument 'equalityTolerance' must not be negative!");
			this.equalityTolerance = equalityTolerance;
		}
	}

}
