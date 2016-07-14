package org.chronos.chronodb.api.builder.query;

/**
 * A base interface for the fluent {@link QueryBuilder} API.
 *
 * <p>
 * Specifies a couple of methods several builders have in common.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @param <SELF>
 *            The dynamic type of <code>this</code> to return for method chaining.
 */
public interface QueryBaseBuilder<SELF extends QueryBaseBuilder<?>> {

	/**
	 * Encapsulates the following expressions in a <code>begin</code>-<code>end</code> brace.
	 *
	 * <p>
	 * This is useful for deciding the precedence of the <code>and</code> and <code>or</code> operators.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().begin().where("name").contains("Hello").or().where("name").contains("World").end().and().where("age")
	 * 		.isGreaterThanOrEqualTo(1000).getResult();
	 * </pre>
	 *
	 * @return <code>this</code> for method chaining
	 */
	public SELF begin();

	/**
	 * Terminates the encapsulation of the preceding expressions in a <code>begin</code>-<code>end</code> brace.
	 *
	 * <p>
	 * This is useful for deciding the precedence of the <code>and</code> and <code>or</code> operators.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().not().where("name").contains("Hello").getResult();
	 * </pre>
	 *
	 * @return <code>this</code> for method chaining
	 */
	public SELF end();

	/**
	 * Negates the following expressions.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().begin().where("name").contains("Hello").or().where("name").contains("World").end().and().where("name")
	 * 		.notContains("foo").getResult();
	 * </pre>
	 *
	 * @return <code>this</code> for method chaining
	 */
	public SELF not();

}
