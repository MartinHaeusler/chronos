package org.chronos.chronograph.api.builder.index;

public interface IndexBuilderStarter {

	public VertexIndexBuilder onVertexProperty(String propertyName);

	public EdgeIndexBuilder onEdgeProperty(String propertyName);
}
