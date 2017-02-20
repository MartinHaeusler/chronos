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

	public SELF begin();

	public SELF end();

	public SELF not();

}
