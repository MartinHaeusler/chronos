package org.chronos.chronograph.api.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.builder.index.ElementTypeChoiceIndexBuilder;
import org.chronos.chronograph.api.builder.index.GraphElementIndexBuilder;
import org.chronos.chronograph.api.builder.index.IndexBuilderStarter;
import org.chronos.chronograph.api.structure.ChronoGraph;

import com.google.common.collect.Sets;

/**
 * The {@link ChronoGraphIndexManager} is responsible for managing the secondary indices for a {@link ChronoGraph} instance.
 *
 * <p>
 * You can get the instance of your graph by calling {@link ChronoGraph#getIndexManager()}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoGraphIndexManager {

	// =====================================================================================================================
	// INDEX BUILDING
	// =====================================================================================================================

	/**
	 * Starting point for the fluent graph index creation API.
	 *
	 * <p>
	 * Use method chaining on the returned object, and call {@link GraphElementIndexBuilder#build()} on the last builder to create the new index.
	 *
	 * <p>
	 * Adding a new graph index marks that particular index as {@linkplain #getDirtyIndices() dirty}. When you are done adding all your secondary indices to the graph, call {@link #reindexAll()} in order to build them.
	 *
	 * @return The next builder in the fluent API, for method chaining. Never <code>null</code>.
	 */
	public IndexBuilderStarter create();

	/**
	 * Creates a string index.
	 *
	 * <p>
	 * This method is outdated. Please use:
	 *
	 * <pre>
	 * create().stringIndex()
	 * </pre>
	 *
	 * instead for the same effect.
	 *
	 * @return The index builder, for method chaining. Never <code>null</code>.
	 *
	 * @deprecated Use <code>create().stringIndex()</code> instead.
	 */
	@Deprecated
	public default ElementTypeChoiceIndexBuilder createIndex() {
		return this.create().stringIndex();
	}

	// =====================================================================================================================
	// INDEX METADATA QUERYING
	// =====================================================================================================================

	/**
	 * Returns the currently available secondary indices for the given graph element class.
	 *
	 * @param clazz
	 *            Either <code>{@link Vertex}.class</code> or <code>{@link Edge}.class</code>. Must not be <code>null</code>.
	 *
	 * @return The currently available secondary indices for the given graph element class. May be empty, but never <code>null</code>.
	 *
	 * @see #getIndexedVertexProperties()
	 * @see #getIndexedEdgePropertyNames()
	 * @see #getIndexedEdgeProperties()
	 * @see #getIndexedEdgePropertyNames()
	 */
	public Set<ChronoGraphIndex> getIndexedPropertiesOf(Class<? extends Element> clazz);

	/**
	 * Returns the currently available secondary indices for vertices.
	 *
	 * @return The currently available secondary indices for vertices. May be empty, but never <code>null</code>.
	 *
	 * @see #getIndexedVertexPropertyNames()
	 * @see #getIndexedEdgeProperties()
	 * @see #getIndexedEdgePropertyNames()
	 */
	public default Set<ChronoGraphIndex> getIndexedVertexProperties() {
		return getIndexedPropertiesOf(Vertex.class);
	}

	/**
	 * Returns the names (keys) of the vertex properties that are currently part of a secondary index.
	 *
	 * @return The set of indexed vertex property names (keys). May be empty, but never <code>null</code>.
	 *
	 * @see #getIndexedVertexProperties()
	 * @see #getIndexedEdgeProperties()
	 * @see #getIndexedEdgePropertyNames()
	 */
	public default Set<String> getIndexedVertexPropertyNames() {
		Set<ChronoGraphIndex> indices = this.getIndexedVertexProperties();
		return indices.stream().map(idx -> idx.getIndexedProperty()).collect(Collectors.toSet());
	}

	/**
	 * Returns the currently available secondary indices for edges.
	 *
	 * @return The currently available secondary indices for edges. May be empty, but never <code>null</code>.
	 *
	 * @see #getIndexedVertexProperties()
	 * @see #getIndexedVertexPropertyNames()
	 * @see #getIndexedEdgePropertyNames()
	 */
	public default Set<ChronoGraphIndex> getIndexedEdgeProperties() {
		return getIndexedPropertiesOf(Edge.class);
	}

	/**
	 * Returns the names (keys) of the edge properties that are currently part of a secondary index.
	 *
	 * @return The set of indexed edge property names (keys). May be empty, but never <code>null</code>.
	 *
	 * @see #getIndexedVertexProperties()
	 * @see #getIndexedVertexPropertyNames()
	 * @see #getIndexedEdgeProperties()
	 */
	public default Set<String> getIndexedEdgePropertyNames() {
		Set<ChronoGraphIndex> indices = this.getIndexedEdgeProperties();
		return indices.stream().map(idx -> idx.getIndexedProperty()).collect(Collectors.toSet());
	}

	/**
	 * Returns the set of all currently available secondary graph indices.
	 *
	 * @return The set of all secondary graph indices. May be empty, but never <code>null</code>.
	 */
	public default Set<ChronoGraphIndex> getAllIndices() {
		Set<ChronoGraphIndex> allIndices = Sets.newHashSet();
		allIndices.addAll(this.getIndexedVertexProperties());
		allIndices.addAll(this.getIndexedEdgeProperties());
		return Collections.unmodifiableSet(allIndices);
	}

	/**
	 * Returns the vertex index for the given property name (key).
	 *
	 * @param indexedPropertyName
	 *            The name (key) of the vertex property to get the secondary index for. Must not be <code>null</code>.
	 *
	 * @return The secondary index for the given property, or <code>null</code> if the property is not indexed.
	 */
	public default ChronoGraphIndex getVertexIndex(final String indexedPropertyName) {
		checkNotNull(indexedPropertyName, "Precondition violation - argument 'indexedPropertyName' must not be NULL!");
		Optional<ChronoGraphIndex> maybeIndex = this.getIndexedVertexProperties().stream()
				.filter(index -> index.getIndexedProperty().equals(indexedPropertyName)).findAny();
		return maybeIndex.orElse(null);
	}

	/**
	 * Returns the edge index for the given property name (key).
	 *
	 * @param indexedPropertyName
	 *            The name (key) of the edge property to get the secondary index for. Must not be <code>null</code>.
	 *
	 * @return The secondary index for the given property, or <code>null</code> if the property is not indexed.
	 */
	public default ChronoGraphIndex getEdgeIndex(final String indexedPropertyName) {
		checkNotNull(indexedPropertyName, "Precondition violation - argument 'indexedPropertyName' must not be NULL!");
		Optional<ChronoGraphIndex> maybeIndex = this.getIndexedEdgeProperties().stream()
				.filter(index -> index.getIndexedProperty().equals(indexedPropertyName)).findAny();
		return maybeIndex.orElse(null);
	}

	/**
	 * Checks if the given property name (key) is indexed for the given graph element class.
	 *
	 * @param clazz
	 *            The graph element class to check (either <code>{@link Vertex}.class</code> or <code>{@link Edge}.class</code>). Must not be <code>null</code>.
	 * @param property
	 *            The name (key) of the property to check. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if the given property is indexed for the given graph element class, otherwise <code>false</code>.
	 *
	 * @see #isVertexPropertyIndexed(String)
	 * @see #isEdgePropertyIndexed(String)
	 */
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

	/**
	 * Checks if the given {@link Vertex} property name (key) is indexed or not.
	 *
	 * @param property
	 *            The vertex property name (key) to check. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if the given property is indexed on vertices, otherwise <code>false</code>.
	 *
	 * @see #isEdgePropertyIndexed(String)
	 */
	public default boolean isVertexPropertyIndexed(final String property) {
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		return this.isPropertyIndexed(Vertex.class, property);
	}

	/**
	 * Checks if the given {@link Edge} property name (key) is indexed or not.
	 *
	 * @param property
	 *            The edge property name (key) to check. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if the given property is indexed on edges, otherwise <code>false</code>.
	 *
	 * @see #isVertexPropertyIndexed(String)
	 */
	public default boolean isEdgePropertyIndexed(final String property) {
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		return this.isPropertyIndexed(Edge.class, property);
	}

	// =====================================================================================================================
	// INDEX CONTENT MANIPULATION
	// =====================================================================================================================

	/**
	 * Rebuilds all secondary graph indices from scratch.
	 *
	 * <p>
	 * This operation is mandatory after adding/removing graph indices. Depending on the backend and size of the database, this operation may take considerable amounts of time and should be used with care.
	 */
	public void reindexAll();

	/**
	 * Recreates the index with the given name.
	 *
	 * @param index
	 *            The index to recreate. Must not be <code>null</code>. Must refer to an existing index.
	 *
	 * @deprecated As of Chronos 0.6.8 or later, please use {@link #reindexAll()} instead.
	 */
	@Deprecated
	public void reindex(ChronoGraphIndex index);

	/**
	 * Drops the given graph index.
	 *
	 * <p>
	 * This operation cannot be undone. Use with care.
	 *
	 * @param index
	 *            The index to drop. Must not be <code>null</code>.
	 */
	public void dropIndex(ChronoGraphIndex index);

	/**
	 * Drops all graph indices.
	 *
	 * <p>
	 * This operation cannot be undone. Use with care.
	 */
	public void dropAllIndices();

	/**
	 * Checks if {@linkplain #reindexAll() reindexing} is required or not.
	 *
	 * <p>
	 * Reindexing is required if at least one graph index {@linkplain #getDirtyIndices() is dirty}.
	 *
	 * @return <code>true</code> if at least one graph index requires rebuilding, otherwise <code>false</code>.
	 */
	public boolean isReindexingRequired();

	/**
	 * Returns the set of secondary graph indices that are currently dirty, i.e. require re-indexing.
	 *
	 * @return The set of dirty secondary indices. May be empty, but never <code>null</code>.
	 */
	public Set<ChronoGraphIndex> getDirtyIndices();

}
