package org.chronos.chronograph.api.builder.query;

import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * A base interface for the fluent {@link GraphQueryBuilder} API.
 *
 * <p>
 * Specifies a couple of methods several builders have in common.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 * @param <E>
 *            The type of elements returned by the resulting query.
 * @param <SELF>
 *            The dynamic type of <code>this</code> to return for method chaining.
 */
public interface GraphQueryBaseBuilder<E extends Element, SELF extends GraphQueryBaseBuilder<E, ?>> {

	/**
	 * Starts a new "begin-end" pair.
	 *
	 * <p>
	 * Begin-end blocks are useful when the standard precedences of "and", "or" and "not" are not desired.
	 *
	 * <p>
	 * When calling this method, it is the caller's responsibility to call {@link #end()} as well. Unbalanced calls of {@link #begin()} and {@link #end()} will result in exceptions during query execution.
	 *
	 * @return The next builder step, for method chaining. Never <code>null</code>.
	 */
	public SELF begin();

	/**
	 * Closes a "begin-end" pair.
	 *
	 * <p>
	 * Begin-end blocks are useful when the standard precedences of "and", "or" and "not" are not desired.
	 *
	 * <p>
	 * When calling this method, it is the caller's responsibility to call {@link #begin()} beforehand. Unbalanced calls of {@link #begin()} and {@link #end()} will result in exceptions during query execution.
	 *
	 * @return The next builder step, for method chaining. Never <code>null</code>.
	 */
	public SELF end();

	/**
	 * Negates the condition specified after calling this method.
	 *
	 * <p>
	 * Please note that the precedence order is:
	 * <ol>
	 * <li>{@link #not()}
	 * <li>{@link GraphFinalizableQueryBuilder#and() and()}
	 * <li>{@link GraphFinalizableQueryBuilder#or() or()}
	 * </ol>
	 *
	 * If you require a different ordering, please use {@link #begin()}-{@link #end()} braces.
	 *
	 * @return The next builder step, for method chaining. Never <code>null</code>.
	 */
	public SELF not();

}
