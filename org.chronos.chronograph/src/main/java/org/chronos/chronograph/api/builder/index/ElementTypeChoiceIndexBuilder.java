package org.chronos.chronograph.api.builder.index;

/**
 * A step in the fluent graph index builder API.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ElementTypeChoiceIndexBuilder {

	/**
	 * Creates a new index on the given vertex property.
	 *
	 * @param propertyName
	 *            The name (key) of the vertex property to index. Must not be <code>null</code>.
	 *
	 * @return The next step in the fluent builder, for method chaining. Never <code>null</code>.
	 */
	public VertexIndexBuilder onVertexProperty(String propertyName);

	/**
	 * Creates a new index on the given edge property.
	 *
	 * @param propertyName
	 *            The name (key) of the edge property to index. Must not be <code>null</code>.
	 * @return The next step in the fluent builder, for method chaining. Never <code>null</code>.
	 */
	public EdgeIndexBuilder onEdgeProperty(String propertyName);

}
