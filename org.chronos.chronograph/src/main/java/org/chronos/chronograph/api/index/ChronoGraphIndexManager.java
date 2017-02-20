package org.chronos.chronograph.api.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.builder.index.IndexBuilderStarter;

import com.google.common.collect.Sets;

public interface ChronoGraphIndexManager {

	// =====================================================================================================================
	// INDEX BUILDING
	// =====================================================================================================================

	public IndexBuilderStarter createIndex();

	// =====================================================================================================================
	// INDEX METADATA QUERYING
	// =====================================================================================================================

	public Set<ChronoGraphIndex> getIndexedPropertiesOf(Class<? extends Element> clazz);

	public default Set<ChronoGraphIndex> getIndexedVertexProperties() {
		return getIndexedPropertiesOf(Vertex.class);
	}

	public default Set<String> getIndexedVertexPropertyNames() {
		Set<ChronoGraphIndex> indices = this.getIndexedVertexProperties();
		return indices.stream().map(idx -> idx.getIndexedProperty()).collect(Collectors.toSet());
	}

	public default Set<ChronoGraphIndex> getIndexedEdgeProperties() {
		return getIndexedPropertiesOf(Edge.class);
	}

	public default Set<String> getIndexedEdgePropertyNames() {
		Set<ChronoGraphIndex> indices = this.getIndexedEdgeProperties();
		return indices.stream().map(idx -> idx.getIndexedProperty()).collect(Collectors.toSet());
	}

	public default Set<ChronoGraphIndex> getAllIndices() {
		Set<ChronoGraphIndex> allIndices = Sets.newHashSet();
		allIndices.addAll(this.getIndexedVertexProperties());
		allIndices.addAll(this.getIndexedEdgeProperties());
		return Collections.unmodifiableSet(allIndices);
	}

	public default ChronoGraphIndex getVertexIndex(final String indexedPropertyName) {
		checkNotNull(indexedPropertyName, "Precondition violation - argument 'indexedPropertyName' must not be NULL!");
		Optional<ChronoGraphIndex> maybeIndex = this.getIndexedVertexProperties().stream()
				.filter(index -> index.getIndexedProperty().equals(indexedPropertyName)).findAny();
		return maybeIndex.orElse(null);
	}

	public default ChronoGraphIndex getEdgeIndex(final String indexedPropertyName) {
		checkNotNull(indexedPropertyName, "Precondition violation - argument 'indexedPropertyName' must not be NULL!");
		Optional<ChronoGraphIndex> maybeIndex = this.getIndexedEdgeProperties().stream()
				.filter(index -> index.getIndexedProperty().equals(indexedPropertyName)).findAny();
		return maybeIndex.orElse(null);
	}

	public default boolean isPropertyIndexed(final Class<? extends Element> clazz, final String property) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		if (Vertex.class.isAssignableFrom(clazz)) {
			ChronoGraphIndex index = this.getVertexIndex(property);
			return index != null;
		} else if (Edge.class.isAssignableFrom(clazz)) {
			ChronoGraphIndex index = this.getEdgeIndex(property);
			return index != null;
		} else {
			throw new IllegalArgumentException("Unknown graph element class: '" + clazz.getName() + "'!");
		}
	}

	public default boolean isVertexPropertyIndexed(final String property) {
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		return this.isPropertyIndexed(Vertex.class, property);
	}

	public default boolean isEdgePropertyIndexed(final String property) {
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		return this.isPropertyIndexed(Edge.class, property);
	}

	// =====================================================================================================================
	// INDEX CONTENT MANIPULATION
	// =====================================================================================================================

	public void reindexAll();

	public void reindex(ChronoGraphIndex index);

	public void dropIndex(ChronoGraphIndex index);

	public void dropAllIndices();

	public boolean isReindexingRequired();

	public Set<ChronoGraphIndex> getDirtyIndices();

}
