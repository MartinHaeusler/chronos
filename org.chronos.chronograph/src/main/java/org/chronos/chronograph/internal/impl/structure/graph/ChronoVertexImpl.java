package org.chronos.chronograph.internal.impl.structure.graph;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.api.transaction.ChronoGraphTransactionInternal;
import org.chronos.chronograph.internal.impl.structure.record.EdgeTargetRecord;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record.VertexPropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record.VertexRecord;
import org.chronos.chronograph.internal.impl.util.ChronoGraphElementUtil;
import org.chronos.chronograph.internal.impl.util.ChronoId;
import org.chronos.chronograph.internal.impl.util.ChronoProxyUtil;
import org.chronos.chronograph.internal.impl.util.PredefinedVertexProperty;
import org.chronos.common.base.CCC;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.logging.LogLevel;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class ChronoVertexImpl extends AbstractChronoElement implements Vertex, ChronoVertex {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final SetMultimap<String, ChronoEdge> labelToIncomingEdges = HashMultimap.create();
	private final SetMultimap<String, ChronoEdge> labelToOutgoingEdges = HashMultimap.create();
	private final Map<String, ChronoVertexProperty<?>> properties = Maps.newHashMap();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public ChronoVertexImpl(final String id, final ChronoGraphInternal g, final ChronoGraphTransactionInternal tx,
			final String label) {
		this(g, tx, id, label, false);
	}

	public ChronoVertexImpl(final ChronoGraphInternal g, final ChronoGraphTransactionInternal tx,
			final VertexRecord record) {
		this(g, tx, record.getId(), record.getLabel(), false);
		this.loadRecordContents(record);
	}

	public ChronoVertexImpl(final ChronoGraphInternal g, final ChronoGraphTransactionInternal tx, final String id,
			final String label, final boolean lazy) {
		super(g, tx, id, label, lazy);
	}

	// =================================================================================================================
	// TINKERPOP 3 API
	// =================================================================================================================

	@Override
	public String label() {
		this.checkAccess();
		return super.label();
	}

	@Override
	@SuppressWarnings("unchecked")
	public <V> ChronoVertexProperty<V> property(final String key, final V value) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		this.checkAccess();
		this.logPropertyChange(key, value);
		ChronoVertexProperty<V> property = (ChronoVertexProperty<V>) this.properties.get(key);
		if (property == null) {
			property = new ChronoVertexProperty<>(this, ChronoId.random(), key, value);
			this.changePropertyStatus(key, PropertyStatus.NEW);
			this.properties.put(key, property);
		} else {
			property.set(value);
			this.changePropertyStatus(key, PropertyStatus.MODIFIED);
		}
		this.updateLifecycleStatus(ElementLifecycleStatus.PROPERTY_CHANGED);
		return property;
	}

	@Override
	public ChronoEdge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
		this.checkAccess();
		ElementHelper.validateLabel(label);
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		if (inVertex == null) {
			throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
		}
		Object id = ElementHelper.getIdValue(keyValues).orElse(null);
		boolean userProvidedId = true;
		if (id != null && id instanceof String == false) {
			throw Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
		}
		if (id == null) {
			id = ChronoId.random();
			// we generated the ID ourselves, it did not come from the user
			userProvidedId = false;
		}
		String edgeId = (String) id;
		this.graph.tx().readWrite();
		// assert that we don't already have a graph element with this ID in our transaction cache
		ChronoGraphTransaction graphTx = this.graph.tx().getCurrentTransaction();
		this.logAddEdge(inVertex, edgeId, userProvidedId, label);
		ChronoEdge edge = graphTx.addEdge(this, (ChronoVertex) inVertex, edgeId, userProvidedId, label, keyValues);
		// add it as an outgoing edge to this vertex
		if (edge.outVertex().equals(this) == false) {
			throw new IllegalStateException("Edge is messed up");
		}
		this.labelToOutgoingEdges.put(label, edge);
		// add it as an incoming edge to the target vertex
		ChronoVertexImpl inV = ChronoProxyUtil.resolveVertexProxy(inVertex);
		if (edge.inVertex().equals(inV) == false) {
			throw new IllegalStateException("Edge is messed up");
		}
		inV.labelToIncomingEdges.put(label, edge);
		edge.updateLifecycleStatus(ElementLifecycleStatus.NEW);
		this.updateLifecycleStatus(ElementLifecycleStatus.EDGE_CHANGED);
		inV.updateLifecycleStatus(ElementLifecycleStatus.EDGE_CHANGED);
		return edge;
	}

	@Override
	public <V> ChronoVertexProperty<V> property(final String key, final V value, final Object... keyValues) {
		this.checkAccess();
		return this.property(Cardinality.list, key, value, keyValues);
	}

	@Override
	public <V> ChronoVertexProperty<V> property(final Cardinality cardinality, final String key, final V value,
			final Object... keyValues) {
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		ElementHelper.validateProperty(key, value);
		this.checkAccess();
		Object id = ElementHelper.getIdValue(keyValues).orElse(null);
		if (id != null && id instanceof String == false) {
			// user-supplied ids are not supported
			throw VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
		}
		if (id == null) {
			id = ChronoId.random();
		}
		String propertyId = (String) id;
		// // the "stageVertexProperty" helper method checks the cardinality and the given parameters. If the
		// // cardinality and parameters indicate that an existing property should be returned, the optional is
		// // non-empty.
		// Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality,
		// key,
		// value, keyValues);
		// if (optionalVertexProperty.isPresent()) {
		// // according to cardinality and other parameters, the property already exists, so return it
		// return (ChronoVertexProperty<V>) optionalVertexProperty.get();
		// }
		this.logPropertyChange(key, value);
		ChronoVertexProperty<V> property = new ChronoVertexProperty<>(this, propertyId, key, value);
		ElementHelper.attachProperties(property, keyValues);
		if (this.property(key).isPresent()) {
			this.changePropertyStatus(key, PropertyStatus.MODIFIED);
		} else {
			this.changePropertyStatus(key, PropertyStatus.NEW);
		}
		this.properties.put(key, property);
		this.updateLifecycleStatus(ElementLifecycleStatus.PROPERTY_CHANGED);
		return property;
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
		this.checkAccess();
		switch (direction) {
		case BOTH:
			if (edgeLabels == null || edgeLabels.length <= 0) {
				// return all
				// note that we wrap the internal collections in new hash sets; gremlin specification states
				// that no concurrent modification excpetions should ever be thrown when iterating over edges
				// in a single-threaded program.

				// also note that we do NOT want self-edges (e.g. v1->v1) to appear twice. Therefore, we use
				// a set to eliminate duplicates. Furthermore, Gremlin wants ot have out-edges before in-edges
				// in the iterator, so we use a linked hash set, as it preserves insertion order.
				Set<Edge> edges = Sets.newLinkedHashSet();
				edges.addAll(this.labelToOutgoingEdges.values());
				edges.addAll(this.labelToIncomingEdges.values());
				return edges.iterator();
			} else {
				// return the ones with matching labels
				Set<Edge> edges = Sets.newLinkedHashSet();
				for (String edgeLabel : edgeLabels) {
					edges.addAll(this.labelToOutgoingEdges.get(edgeLabel));
					edges.addAll(this.labelToIncomingEdges.get(edgeLabel));
				}
				return edges.iterator();
			}
		case IN:
			if (edgeLabels == null || edgeLabels.length <= 0) {
				// return all
				// note that we wrap the internal collections in new hash sets; gremlin specification states
				// that no concurrent modification excpetions should ever be thrown when iterating over edges
				// in a single-threaded program.
				Iterator iterator = Sets.newHashSet(this.labelToIncomingEdges.values()).iterator();
				return iterator;
			} else {
				// return the ones with matching labels
				Set<Edge> edges = Sets.newHashSet();
				for (String edgeLabel : edgeLabels) {
					edges.addAll(this.labelToIncomingEdges.get(edgeLabel));
				}
				return edges.iterator();
			}
		case OUT:
			if (edgeLabels == null || edgeLabels.length <= 0) {
				// return all
				// note that we wrap the internal collections in new hash sets; gremlin specification states
				// that no concurrent modification excpetions should ever be thrown when iterating over edges
				// in a single-threaded program.
				Iterator iterator = Sets.newHashSet(this.labelToOutgoingEdges.values()).iterator();
				return iterator;
			} else {
				// return the ones with matching labels
				Set<Edge> edges = Sets.newHashSet();
				for (String edgeLabel : edgeLabels) {
					edges.addAll(this.labelToOutgoingEdges.get(edgeLabel));
				}
				return edges.iterator();
			}
		default:
			throw new UnknownEnumLiteralException(direction);
		}
	}

	@Override
	public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
		this.checkAccess();
		Iterator<Edge> edges = this.edges(direction, edgeLabels);
		return new OtherEndVertexResolvingEdgeIterator(edges);
	}

	@Override
	public <V> VertexProperty<V> property(final String key) {
		this.checkAccess();
		// note: this code is more efficient than the standard implementation in TinkerPop
		// because it avoids the creation of an iterator via #properties(key).
		VertexProperty<V> property = this.getSingleProperty(key);
		if (property == null) {
			return VertexProperty.<V>empty();
		}
		return property;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
		this.checkAccess();
		if (propertyKeys == null || propertyKeys.length <= 0) {
			// if no property keys are given, we have to return ALL properties
			return new PropertiesIterator<>(Sets.newHashSet(this.properties.values()).iterator());
		}
		if (propertyKeys.length == 1) {
			// special common case: only one key is given. This is an optimization that
			// avoids creating a new collection and adding elements to it.
			String propertyKey = propertyKeys[0];
			VertexProperty<V> property = this.getSingleProperty(propertyKey);
			if (property == null) {
				return Collections.emptyIterator();
			} else {
				return Iterators.singletonIterator(property);
			}
		}
		// general case: more than one key is requested
		Set<VertexProperty<V>> matchingProperties = Sets.newHashSet();
		for (String propertyKey : propertyKeys) {
			VertexProperty<?> property = this.getSingleProperty(propertyKey);
			if (property != null) {
				matchingProperties.add((VertexProperty<V>) property);
			}
		}
		return matchingProperties.iterator();
	}

	@Override
	public void remove() {
		this.checkAccess();
		this.logVertexRemove();
		// first, remove all incoming and outgoing edges
		Iterator<Edge> edges = this.edges(Direction.BOTH);
		while (edges.hasNext()) {
			Edge edge = edges.next();
			edge.remove();
		}
		// then, remove the vertex itself
		super.remove();
	}

	@Override
	public String toString() {
		return StringFactory.vertexString(this);
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	protected void loadRecordContents(final VertexRecord record) {
		this.labelToIncomingEdges.clear();
		this.labelToOutgoingEdges.clear();
		this.properties.clear();
		if (record == null) {
			this.updateLifecycleStatus(ElementLifecycleStatus.REMOVED);
			return;
		} else {
			this.updateLifecycleStatus(ElementLifecycleStatus.PERSISTED);
		}
		this.clearPropertyStatusCache();
		this.label = record.getLabel();
		for (VertexPropertyRecord pRecord : record.getProperties()) {
			String pKey = pRecord.getKey();
			Object pVal = pRecord.getValue();
			String propertyId = pRecord.getId();
			ChronoVertexProperty<?> property = new ChronoVertexProperty<>(this, propertyId, pKey, pVal, true);
			for (Entry<String, PropertyRecord> pEntry : pRecord.getProperties().entrySet()) {
				String metaKey = pEntry.getKey();
				PropertyRecord metaProperty = pEntry.getValue();
				property.property(metaKey, metaProperty.getValue());
			}
			this.properties.put(property.key(), property);
		}
		for (Entry<String, EdgeTargetRecord> entry : record.getIncomingEdgesByLabel().entries()) {
			String label = entry.getKey();
			EdgeTargetRecord eRecord = entry.getValue();
			ChronoEdge edge = this.owningTransaction.loadIncomingEdgeFromEdgeTargetRecord(this, label, eRecord);
			this.labelToIncomingEdges.put(edge.label(), edge);
		}
		for (Entry<String, EdgeTargetRecord> entry : record.getOutgoingEdgesByLabel().entries()) {
			String label = entry.getKey();
			EdgeTargetRecord eRecord = entry.getValue();
			ChronoEdge edge = this.owningTransaction.loadOutgoingEdgeFromEdgeTargetRecord(this, label, eRecord);
			this.labelToOutgoingEdges.put(edge.label(), edge);
		}
	}

	@Override
	public void removeProperty(final String key) {
		this.checkAccess();
		this.logPropertyRemove(key);
		this.properties.remove(key);
		this.changePropertyStatus(key, PropertyStatus.REMOVED);
		this.updateLifecycleStatus(ElementLifecycleStatus.PROPERTY_CHANGED);
	}

	public VertexRecord toRecord() {
		this.checkAccess();
		String id = this.id();
		String label = this.label();
		return new VertexRecord(
				id, label,
				this.labelToIncomingEdges, this.labelToOutgoingEdges,
				this.properties);
	}

	@Override
	public void updateLifecycleStatus(final ElementLifecycleStatus status) {
		super.updateLifecycleStatus(status);
		if (this.isModificationCheckActive()) {
			if (status.isDirty()) {
				this.getTransactionContext().markVertexAsModified(this);
			}
		}
	}

	public void removeEdge(final ChronoEdgeImpl chronoEdge) {
		checkNotNull(chronoEdge, "Precondition violation - argument 'chronoEdge' must not be NULL!");
		this.checkAccess();
		boolean changed = false;
		if (chronoEdge.inVertex().equals(this)) {
			// incoming edge
			// remove whatever edge representation has been there with this edge-id
			boolean removed = this.labelToIncomingEdges.remove(chronoEdge.label(), chronoEdge);
			if (removed == false) {
				throw new IllegalStateException("Graph is inconsistent - failed to remove edge from adjacent vertex!");
			}
			changed = true;
		}
		// note: this vertex can be in AND out vertex (self-edge!)
		if (chronoEdge.outVertex().equals(this)) {
			// outgoing edge
			// remove whatever edge representation has been there with this edge-id
			boolean removed = this.labelToOutgoingEdges.remove(chronoEdge.label(), chronoEdge);
			if (removed == false) {
				throw new IllegalStateException("Graph is inconsistent - failed to remove edge from adjacent vertex!");
			}
			changed = removed || changed;
		}
		if (changed) {
			this.updateLifecycleStatus(ElementLifecycleStatus.EDGE_CHANGED);
		}
	}

	@Override
	protected void reloadFromDatabase() {
		String id = this.id();
		ChronoGraphTransaction tx = this.getOwningTransaction();
		VertexRecord vRecord = tx.getBackingDBTransaction().get(ChronoGraphConstants.KEYSPACE_VERTEX, id.toString());
		this.withoutModificationCheck(() -> {
			this.loadRecordContents(vRecord);
		});
		this.updateLifecycleStatus(ElementLifecycleStatus.PERSISTED);
		this.getTransactionContext().registerLoadedVertex(this);
	}

	@SuppressWarnings({ "unchecked" })
	private <V> VertexProperty<V> getSingleProperty(final String propertyKey) {
		PredefinedVertexProperty<V> predefinedProperty = ChronoGraphElementUtil.asPredefinedVertexProperty(this,
				propertyKey);
		if (predefinedProperty != null) {
			return predefinedProperty;
		}
		return (VertexProperty<V>) this.properties.get(propertyKey);
	}

	// =====================================================================================================================
	// DEBUG OUTPUT
	// =====================================================================================================================

	private void logPropertyChange(final String key, final Object value) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
			// log level is higher than trace, no need to prepare the message
			return;
		}
		// prepare some debug output
		StringBuilder messageBuilder = new StringBuilder();
		messageBuilder.append("[GRAPH MODIFICATION] Setting Property '");
		messageBuilder.append(key);
		messageBuilder.append("' ");
		if (this.property(key).isPresent()) {
			messageBuilder.append("from '");
			messageBuilder.append(this.value(key).toString());
			messageBuilder.append("' to '");
			messageBuilder.append(value.toString());
			messageBuilder.append("' ");
		} else {
			messageBuilder.append("to '");
			messageBuilder.append(value.toString());
			messageBuilder.append("' (new property) ");
		}
		messageBuilder.append(" on Vertex ");
		messageBuilder.append(this.toString());
		messageBuilder.append(" (Object ID: ");
		messageBuilder.append(System.identityHashCode(this));
		messageBuilder.append(")");
		ChronoLogger.logTrace(messageBuilder.toString());
	}

	private void logVertexRemove() {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
			// log level is higher than trace, no need to prepare the message
			return;
		}
		// prepare some debug output
		StringBuilder messageBuilder = new StringBuilder();
		messageBuilder.append("[GRAPH MODIFICATION] Removing Vertex ");
		messageBuilder.append(this.toString());
		messageBuilder.append(" (Object ID: ");
		messageBuilder.append(System.identityHashCode(this));
		messageBuilder.append(")");
		ChronoLogger.logTrace(messageBuilder.toString());
	}

	private void logPropertyRemove(final String key) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
			// log level is higher than trace, no need to prepare the message
			return;
		}
		// prepare some debug output
		StringBuilder messageBuilder = new StringBuilder();
		messageBuilder.append("[GRAPH MODIFICATION] Removing Property '");
		messageBuilder.append(key);
		messageBuilder.append("' from Vertex ");
		messageBuilder.append(this.toString());
		messageBuilder.append(" (Object ID: ");
		messageBuilder.append(System.identityHashCode(this));
		messageBuilder.append(")");
		ChronoLogger.logTrace(messageBuilder.toString());
	}

	private void logAddEdge(final Vertex inVertex, final String edgeId, final boolean userProvidedId,
			final String label) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
			// log level is higher than trace, no need to prepare the message
			return;
		}
		// prepare some debug output
		StringBuilder messageBuilder = new StringBuilder();
		messageBuilder.append("[GRAPH MODIFICATION] Adding Edge. From Vertex: '");
		messageBuilder.append(this.toString());
		messageBuilder.append(" (Object ID: ");
		messageBuilder.append(System.identityHashCode(this));
		messageBuilder.append(") to Vertex '");
		messageBuilder.append(inVertex.toString());
		messageBuilder.append(" (Object ID: ");
		messageBuilder.append(System.identityHashCode(inVertex));
		messageBuilder.append(") with ");
		if (userProvidedId) {
			messageBuilder.append("user-provided ");
		} else {
			messageBuilder.append("auto-generated ");
		}
		messageBuilder.append("Edge ID '");
		messageBuilder.append(edgeId);
		messageBuilder.append("' and label '");
		messageBuilder.append(label);
		messageBuilder.append("'");
		ChronoLogger.logTrace(messageBuilder.toString());
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private class OtherEndVertexResolvingEdgeIterator implements Iterator<Vertex> {

		private final Iterator<Edge> edgeIterator;

		private OtherEndVertexResolvingEdgeIterator(final Iterator<Edge> edgeIterator) {
			this.edgeIterator = edgeIterator;
		}

		@Override
		public boolean hasNext() {
			return this.edgeIterator.hasNext();
		}

		@Override
		public Vertex next() {
			Edge edge = this.edgeIterator.next();
			Vertex inV = edge.inVertex();
			if (inV.equals(ChronoVertexImpl.this)) {
				return edge.outVertex();
			} else {
				return edge.inVertex();
			}
		}

	}

	private class PropertiesIterator<V> implements Iterator<VertexProperty<V>> {

		private final Iterator<ChronoVertexProperty<?>> iterator;

		private PropertiesIterator(final Iterator<ChronoVertexProperty<?>> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return this.iterator.hasNext();
		}

		@Override
		@SuppressWarnings("unchecked")
		public VertexProperty<V> next() {
			return (VertexProperty<V>) this.iterator.next();
		}
	}

}
