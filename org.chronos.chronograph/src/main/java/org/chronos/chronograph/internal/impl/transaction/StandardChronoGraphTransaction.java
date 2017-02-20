package org.chronos.chronograph.internal.impl.transaction;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.PutOption;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ElementLifecycleStatus;
import org.chronos.chronograph.internal.impl.structure.record.EdgeRecord;
import org.chronos.chronograph.internal.impl.structure.record.VertexRecord;
import org.chronos.chronograph.internal.impl.util.ChronoId;
import org.chronos.chronograph.internal.impl.util.ChronoProxyUtil;
import org.chronos.common.base.CCC;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.logging.LogLevel;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class StandardChronoGraphTransaction implements ChronoGraphTransaction {

	private final String transactionId;
	private final ChronoGraph graph;
	private final ChronoDBTransaction backendTransaction;
	private final GraphTransactionContext context;
	private final ChronoGraphQueryProcessor queryProcessor;

	private long rollbackCount;

	public StandardChronoGraphTransaction(final ChronoGraph graph, final ChronoDBTransaction backendTransaction) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(backendTransaction, "Precondition violation - argument 'backendTransaction' must not be NULL!");
		this.transactionId = UUID.randomUUID().toString();
		this.graph = graph;
		this.backendTransaction = backendTransaction;
		this.context = new GraphTransactionContext();
		this.rollbackCount = 0L;
		this.queryProcessor = new ChronoGraphQueryProcessor(this);
	}

	// =====================================================================================================================
	// METADATA
	// =====================================================================================================================

	@Override
	public ChronoDBTransaction getBackingDBTransaction() {
		return this.backendTransaction;
	}

	@Override
	public GraphTransactionContext getContext() {
		return this.context;
	}

	@Override
	public String getTransactionId() {
		return this.transactionId;
	}

	@Override
	public long getRollbackCount() {
		return this.rollbackCount;
	}

	@Override
	public ChronoGraph getGraph() {
		return this.graph;
	}

	@Override
	public boolean isThreadedTx() {
		// can be overridden in subclasses.
		return false;
	}

	@Override
	public boolean isThreadLocalTx() {
		// can be overridden in subclasses.
		return true;
	}

	@Override
	public boolean isOpen() {
		// can be overridden in subclasses.
		return true;
	}

	// =====================================================================================================================
	// COMMIT & ROLLBACK
	// =====================================================================================================================

	@Override
	public void commit() {
		this.commit(null);
	}

	@Override
	public void commit(final Object metadata) {
		this.mapModifiedVerticesToChronoDB();
		this.mapModifiedEdgesToChronoDB();
		this.mapModifiedGraphVariablesToChronoDB();
		// commit the transaction
		this.getBackingDBTransaction().commit(metadata);
		// clear the transaction context
		this.context.clear();
	}

	@Override
	public void commitIncremental() {
		this.mapModifiedVerticesToChronoDB();
		this.mapModifiedEdgesToChronoDB();
		this.mapModifiedGraphVariablesToChronoDB();
		// commit the transaction
		this.getBackingDBTransaction().commitIncremental();
		// clear the transaction context
		this.context.clear();
		// we "treat" the incremental commit like a rollback, because
		// we want existing proxies to re-register themselves at the
		// context (which we just had to clear).
		this.rollbackCount++;
	}

	@Override
	public void rollback() {
		this.context.clear();
		this.rollbackCount++;
		this.getBackingDBTransaction().rollback();
	}

	// =====================================================================================================================
	// QUERY METHODS
	// =====================================================================================================================

	@Override
	public Iterator<Vertex> vertices(final Object... vertexIds) {
		if (vertexIds == null || vertexIds.length <= 0) {
			// query all vertices... this is bad.
			ChronoLogger.logWarning("Query requires iterating over all vertices."
					+ " For better performance, use 'has(...)' clauses in your gremlin.");
			return this.getAllVerticesIterator();
		}
		if (this.areAllOfType(String.class, vertexIds)) {
			// retrieve some vertices by IDs
			List<String> chronoVertexIds = Lists.newArrayList(vertexIds).stream().map(id -> (String) id)
					.collect(Collectors.toList());
			return this.getVerticesIterator(chronoVertexIds);
		}
		if (this.areAllOfType(Vertex.class, vertexIds)) {
			// vertices were passed as arguments -> extract their IDs and query them
			List<String> ids = Lists.newArrayList(vertexIds).stream().map(v -> ((String) ((Vertex) v).id()))
					.collect(Collectors.toList());
			return this.getVerticesIterator(ids);
		}
		// in any other case, something wrong was passed as argument...
		throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
	}

	@Override
	public Iterator<Vertex> getAllVerticesIterator() {
		return this.queryProcessor.getAllVerticesIterator();
	}

	@Override
	public Iterator<Vertex> getVerticesIterator(final Iterable<String> chronoVertexIds,
			final ElementLoadMode loadMode) {
		checkNotNull(chronoVertexIds, "Precondition violation - argument 'chronoVertexIds' must not be NULL!");
		checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
		return this.queryProcessor.getVerticesIterator(chronoVertexIds, loadMode);
	}

	@Override
	public Iterator<Vertex> getVerticesByProperties(final Map<String, String> propertyKeyToPropertyValue) {
		checkNotNull(propertyKeyToPropertyValue,
				"Precondition violation - argument 'propertyKeyToPropertyValue' must not be NULL!");
		return this.queryProcessor.getVerticesByProperties(propertyKeyToPropertyValue);
	}

	@Override
	public Set<Vertex> evaluateVertexQuery(final ChronoDBQuery query) {
		checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
		return this.queryProcessor.evaluateVertexQuery(query);
	}

	@Override
	public Iterator<Edge> edges(final Object... edgeIds) {
		if (edgeIds == null || edgeIds.length <= 0) {
			// query all edges... this is bad.
			ChronoLogger.logWarning("Query requires iterating over all edges."
					+ " For better performance, use 'has(...)' clauses in your gremlin.");
			return this.getAllEdgesIterator();
		}
		if (this.areAllOfType(String.class, edgeIds)) {
			// retrieve some edges by IDs
			List<String> chronoEdgeIds = Lists.newArrayList(edgeIds).stream().map(id -> (String) id)
					.collect(Collectors.toList());
			return this.getEdgesIterator(chronoEdgeIds);
		}
		if (this.areAllOfType(Edge.class, edgeIds)) {
			// edges were passed as arguments -> extract their IDs and query them
			List<String> ids = Lists.newArrayList(edgeIds).stream().map(e -> ((String) ((Edge) e).id()))
					.collect(Collectors.toList());
			return this.getEdgesIterator(ids);
		}
		// in any other case, something wrong was passed as argument...
		throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
	}

	@Override
	public Iterator<Edge> getAllEdgesIterator() {
		return this.queryProcessor.getAllEdgesIterator();
	}

	@Override
	public Iterator<Edge> getEdgesIterator(final Iterable<String> edgeIds) {
		checkNotNull(edgeIds, "Precondition violation - argument 'edgeIds' must not be NULL!");
		return this.queryProcessor.getEdgesIterator(edgeIds);
	}

	@Override
	public Iterator<Edge> getEdgesByProperties(final Map<String, String> propertyKeyToPropertyValue) {
		checkNotNull(propertyKeyToPropertyValue,
				"Precondition violation - argument 'propertyKeyToPropertyValue' must not be NULL!");
		return this.queryProcessor.getEdgesByProperties(propertyKeyToPropertyValue);
	}

	@Override
	public Set<Edge> evaluateEdgeQuery(final ChronoDBQuery query) {
		checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
		return this.queryProcessor.evaluateEdgeQuery(query);
	}

	// =====================================================================================================================
	// TEMPORAL QUERY METHODS
	// =====================================================================================================================

	@Override
	public Iterator<Long> getVertexHistory(final Object vertexId) {
		checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
		if (vertexId instanceof Vertex) {
			return this.getVertexHistory((Vertex) vertexId);
		}
		if (vertexId instanceof String) {
			return this.getVertexHistory((String) vertexId);
		}
		throw new IllegalArgumentException("The given object is no valid vertex id: " + vertexId);
	}

	@Override
	public Iterator<Long> getVertexHistory(final Vertex vertex) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		ChronoVertex chronoVertex = (ChronoVertex) vertex;
		return this.getVertexHistory(chronoVertex.id());
	}

	@Override
	public Iterator<Long> getEdgeHistory(final Object edgeId) {
		checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
		if (edgeId instanceof Edge) {
			return this.getEdgeHistory((Edge) edgeId);
		}
		if (edgeId instanceof String) {
			return this.getEdgeHistory((String) edgeId);
		}
		throw new IllegalArgumentException("The given object is no valid edge id: " + edgeId);
	}

	@Override
	public Iterator<Long> getEdgeHistory(final Edge edge) {
		checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
		ChronoEdge chronoEdge = (ChronoEdge) edge;
		return this.getEdgeHistory(chronoEdge.id());
	}

	@Override
	public Iterator<Pair<Long, String>> getVertexModificationsBetween(final long timestampLowerBound,
			final long timestampUpperBound) {
		checkArgument(timestampLowerBound >= 0,
				"Precondition violation - argument 'timestampLowerBound' must not be negative!");
		checkArgument(timestampUpperBound >= 0,
				"Precondition violation - argument 'timestampUpperBound' must not be negative!");
		checkArgument(timestampLowerBound <= timestampUpperBound,
				"Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
		checkArgument(timestampLowerBound <= this.getTimestamp(),
				"Precondition violation - argument 'timestampLowerBound' must not exceed the transaction timestamp!");
		checkArgument(timestampUpperBound <= this.getTimestamp(),
				"Precondition violation - argument 'timestampUpperBound' must not exceed the transaction timestamp!");
		Iterator<TemporalKey> temporalKeyIterator = this.getBackingDBTransaction().getModificationsInKeyspaceBetween(
				ChronoGraphConstants.KEYSPACE_VERTEX, timestampLowerBound, timestampUpperBound);
		return Iterators.transform(temporalKeyIterator, tk -> Pair.of(tk.getTimestamp(), tk.getKey()));
	}

	@Override
	public Iterator<Pair<Long, String>> getEdgeModificationsBetween(final long timestampLowerBound,
			final long timestampUpperBound) {
		checkArgument(timestampLowerBound >= 0,
				"Precondition violation - argument 'timestampLowerBound' must not be negative!");
		checkArgument(timestampUpperBound >= 0,
				"Precondition violation - argument 'timestampUpperBound' must not be negative!");
		checkArgument(timestampLowerBound <= timestampUpperBound,
				"Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
		checkArgument(timestampLowerBound <= this.getTimestamp(),
				"Precondition violation - argument 'timestampLowerBound' must not exceed the transaction timestamp!");
		checkArgument(timestampUpperBound <= this.getTimestamp(),
				"Precondition violation - argument 'timestampUpperBound' must not exceed the transaction timestamp!");
		Iterator<TemporalKey> temporalKeyIterator = this.getBackingDBTransaction().getModificationsInKeyspaceBetween(
				ChronoGraphConstants.KEYSPACE_EDGE, timestampLowerBound, timestampUpperBound);
		return Iterators.transform(temporalKeyIterator, tk -> Pair.of(tk.getTimestamp(), tk.getKey()));
	}

	@Override
	public Object getCommitMetadata(final long commitTimestamp) {
		checkArgument(commitTimestamp >= 0,
				"Precondition violation - argument 'commitTimestamp' must not be negative!");
		checkArgument(commitTimestamp <= this.getTimestamp(),
				"Precondition violation - argument 'commitTimestamp' must not be larger than the transaction timestamp!");
		return this.getBackingDBTransaction().getCommitMetadata(commitTimestamp);
	}

	// =================================================================================================================
	// ELEMENT CREATION METHODS
	// =================================================================================================================

	@Override
	public ChronoVertex addVertex(final Object... keyValues) {
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		Object id = ElementHelper.getIdValue(keyValues).orElse(null);
		boolean userProvidedId = true;
		if (id != null && id instanceof String == false) {
			throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
		}
		if (id == null) {
			id = ChronoId.random();
			// we generated the ID ourselves, it did not come from the user
			userProvidedId = false;
		}
		String vertexId = (String) id;
		String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
		if (userProvidedId && this.getGraph().getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled()) {
			// assert that we don't already have a graph element with this ID in our transaction cache
			ChronoVertex modifiedVertex = this.getContext().getModifiedVertex(vertexId);
			if (modifiedVertex != null) {
				throw Graph.Exceptions.vertexWithIdAlreadyExists(vertexId);
			}
			// assert that we don't already have a graph element with this ID in our persistence
			ChronoDBTransaction backingTx = this.getBackingDBTransaction();
			boolean vertexIdAlreadyExists = backingTx.exists(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId);
			if (vertexIdAlreadyExists) {
				throw Graph.Exceptions.vertexWithIdAlreadyExists(vertexId);
			}
		}
		this.logAddVertex(vertexId, userProvidedId);
		ChronoVertexImpl vertex = new ChronoVertexImpl(vertexId, this.graph, this, label);
		ElementHelper.attachProperties(vertex, keyValues);
		vertex.updateLifecycleStatus(ElementLifecycleStatus.PROPERTY_CHANGED);
		return this.context.getOrCreateVertexProxy(vertex);
	}

	@Override
	public ChronoEdge addEdge(final ChronoVertex outVertex, final ChronoVertex inVertex, final String id,
			final boolean isUserProvidedId, final String label, final Object... keyValues) {
		if (isUserProvidedId && this.getGraph().getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled()) {
			// assert that we don't already have a graph element with this ID in our transaction cache
			ChronoEdge modifiedEdge = this.getContext().getModifiedEdge(id);
			if (modifiedEdge != null) {
				throw Graph.Exceptions.edgeWithIdAlreadyExists(id);
			}
			// assert that we don't already have a graph element with this ID in our persistence
			ChronoDBTransaction backingTx = this.getBackingDBTransaction();
			boolean edgeIdAlreadyExists = backingTx.exists(ChronoGraphConstants.KEYSPACE_EDGE, id);
			if (edgeIdAlreadyExists) {
				throw Graph.Exceptions.edgeWithIdAlreadyExists(id);
			}
		}
		// create the edge
		ChronoVertexImpl outV = ChronoProxyUtil.resolveVertexProxy(outVertex);
		ChronoVertexImpl inV = ChronoProxyUtil.resolveVertexProxy(inVertex);
		ChronoEdgeImpl edge = ChronoEdgeImpl.create(id, outV, label, inV);
		// set the properties (if any)
		if (keyValues != null && keyValues.length > 0) {
			for (int i = 0; i < keyValues.length; i += 2) {
				if (keyValues[i] instanceof T == false) {
					String key = (String) keyValues[i];
					Object value = keyValues[i + 1];
					edge.property(key, value);
				}
			}
		}
		edge.updateLifecycleStatus(ElementLifecycleStatus.NEW);
		return this.context.getOrCreateEdgeProxy(edge);
	}

	// =====================================================================================================================
	// EQUALS & HASH CODE
	// =====================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.transactionId == null ? 0 : this.transactionId.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		StandardChronoGraphTransaction other = (StandardChronoGraphTransaction) obj;
		if (this.transactionId == null) {
			if (other.transactionId != null) {
				return false;
			}
		} else if (!this.transactionId.equals(other.transactionId)) {
			return false;
		}
		return true;
	}

	// =====================================================================================================================
	// LOADING METHODS
	// =====================================================================================================================

	public ChronoVertexImpl loadVertex(final String id, final ElementLoadMode loadMode) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		// first, try to find the vertex in our 'modified vertices' cache
		ChronoVertexImpl modifiedVertex = this.context.getModifiedVertex(id);
		if (modifiedVertex != null) {
			// the given vertex is in our 'modified vertices' cache; reuse it
			return modifiedVertex;
		}
		// then, try to find it in our 'already loaded' cache
		ChronoVertexImpl loadedVertex = this.context.getLoadedVertexForId(id);
		if (loadedVertex != null) {
			// the vertex was already loaded in this transaction; return the same instance
			return loadedVertex;
		}
		ChronoVertexImpl vertex = null;
		switch (loadMode) {
		case EAGER:
			// we are not sure if there is a vertex in the database for the given id. We need
			// to make a load attempt to make sure it exists.
			ChronoDBTransaction tx = this.getBackingDBTransaction();
			VertexRecord record = tx.get(ChronoGraphConstants.KEYSPACE_VERTEX, id.toString());
			// load the vertex from the database
			if (record == null) {
				return null;
			}
			vertex = new ChronoVertexImpl(this.graph, this, record);
			break;
		case LAZY:
			// we can trust that there actually IS a vertex in the database for the given ID,
			// but we want to load it lazily when a vertex property is first accessed.
			vertex = new ChronoVertexImpl(this.graph, this, id, Vertex.DEFAULT_LABEL, true);
			vertex.updateLifecycleStatus(ElementLifecycleStatus.PERSISTED);
			break;
		default:
			throw new UnknownEnumLiteralException(loadMode);
		}
		// register the loaded instance
		this.context.registerLoadedVertex(vertex);
		return vertex;
	}

	public ChronoEdgeImpl loadEdge(final String id) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		// first, try to find the edge in our 'modified edges' cache
		ChronoEdgeImpl modifiedEdge = this.context.getModifiedEdge(id);
		if (modifiedEdge != null) {
			// the given vertex is in our 'modified edges' cache; reuse it
			return modifiedEdge;
		}
		// then, try to find it in our 'already loaded' cache
		ChronoEdgeImpl loadedEdge = this.context.getLoadedEdgeForId(id);
		if (loadedEdge != null) {
			// the edge was already loaded in this transaction; return the same instance
			return loadedEdge;
		}
		// load the edge from the database
		ChronoDBTransaction tx = this.getBackingDBTransaction();
		EdgeRecord record = tx.get(ChronoGraphConstants.KEYSPACE_EDGE, id.toString());
		if (record == null) {
			return null;
		}
		ChronoEdgeImpl edge = ChronoEdgeImpl.create(this.graph, this, record);
		// register the loaded edge
		this.context.registerLoadedEdge(edge);
		return edge;
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private boolean areAllOfType(final Class<?> clazz, final Object... objects) {
		for (Object object : objects) {
			if (clazz.isInstance(object) == false) {
				return false;
			}
		}
		return true;
	}

	private Iterator<Long> getVertexHistory(final String vertexId) {
		checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
		ChronoDBTransaction tx = this.getBackingDBTransaction();
		return tx.history(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId);
	}

	private Iterator<Long> getEdgeHistory(final String edgeId) {
		checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
		ChronoDBTransaction tx = this.getBackingDBTransaction();
		return tx.history(ChronoGraphConstants.KEYSPACE_EDGE, edgeId);
	}

	private void mapModifiedVerticesToChronoDB() {
		// get the backing transaction
		ChronoDBTransaction tx = this.getBackingDBTransaction();
		// read the set of modified vertices
		Set<ChronoVertexImpl> modifiedVertices = this.context.getModifiedVertices();
		// write each vertex into a key-value pair in the transaction
		for (ChronoVertexImpl vertex : modifiedVertices) {
			String vertexId = vertex.id().toString();
			if (vertex.isRemoved()) {
				tx.remove(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId);
			} else {
				VertexRecord record = vertex.toRecord();
				if (vertex.getStatus().equals(ElementLifecycleStatus.EDGE_CHANGED)) {
					tx.put(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId, record, PutOption.NO_INDEX);
				} else {
					tx.put(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId, record);
				}
			}
		}
	}

	private void mapModifiedEdgesToChronoDB() {
		// get the backing transaction
		ChronoDBTransaction tx = this.getBackingDBTransaction();
		// read the set of modified edges
		Set<ChronoEdgeImpl> modifiedEdges = this.context.getModifiedEdges();
		// write each edge into a key-value pair in the transaction
		for (ChronoEdgeImpl edge : modifiedEdges) {
			String edgeId = edge.id().toString();
			if (edge.isRemoved()) {
				tx.remove(ChronoGraphConstants.KEYSPACE_EDGE, edgeId);
			} else {
				EdgeRecord record = edge.toRecord();
				tx.put(ChronoGraphConstants.KEYSPACE_EDGE, edgeId, record);
			}
		}
	}

	private void mapModifiedGraphVariablesToChronoDB() {
		// get the backing transaction
		ChronoDBTransaction tx = this.getBackingDBTransaction();
		// write the modifications of graph variables into the transaction context
		for (String variableName : this.context.getModifiedVariables()) {
			if (this.context.isVariableRemoved(variableName)) {
				tx.remove(ChronoGraphConstants.KEYSPACE_VARIABLES, variableName);
			} else {
				tx.put(ChronoGraphConstants.KEYSPACE_VARIABLES, variableName,
						this.context.getModifiedVariableValue(variableName));
			}
		}
	}

	// =====================================================================================================================
	// DEBUG LOGGING
	// =====================================================================================================================

	private void logAddVertex(final String vertexId, final boolean isUserProvided) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
			// log level is higher than trace, no need to prepare the message
			return;
		}
		// prepare some debug output
		StringBuilder messageBuilder = new StringBuilder();
		messageBuilder.append("[GRAPH MODIFICATION] Adding Vertex with ");
		if (isUserProvided) {
			messageBuilder.append("user-provided ");
		} else {
			messageBuilder.append("auto-generated ");
		}
		messageBuilder.append("ID '");
		messageBuilder.append(vertexId);
		messageBuilder.append("' to graph.");
		ChronoLogger.logTrace(messageBuilder.toString());
	}

}
