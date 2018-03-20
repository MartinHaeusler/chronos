package org.chronos.chronograph.internal.impl.structure.graph;

import static com.google.common.base.Preconditions.*;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.api.transaction.ChronoGraphTransactionInternal;
import org.chronos.chronograph.internal.impl.structure.record.EdgeRecord;
import org.chronos.chronograph.internal.impl.structure.record.EdgeTargetRecord;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.chronograph.internal.impl.util.ChronoGraphElementUtil;
import org.chronos.chronograph.internal.impl.util.ChronoProxyUtil;
import org.chronos.chronograph.internal.impl.util.PredefinedProperty;
import org.chronos.common.base.CCC;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.logging.LogLevel;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ChronoEdgeImpl extends AbstractChronoElement implements Edge, ChronoEdge {

	public static ChronoEdgeImpl create(final ChronoGraphInternal graph, final ChronoGraphTransactionInternal tx,
			final EdgeRecord record) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
		String id = record.getId();
		String outV = record.getOutVertexId();
		String label = record.getLabel();
		String inV = record.getInVertexId();
		Set<PropertyRecord> properties = record.getProperties();
		ChronoEdgeImpl edge = new ChronoEdgeImpl(graph, tx, id, outV, label, inV, properties);
		edge.updateLifecycleStatus(ElementLifecycleStatus.PERSISTED);
		return edge;
	}

	public static ChronoEdgeImpl create(final String id, final ChronoVertexImpl outV, final String label,
			final ChronoVertexImpl inV) {
		checkNotNull(outV, "Precondition violation - argument 'outV' must not be NULL!");
		ElementHelper.validateLabel(label);
		checkNotNull(inV, "Precondition violation - argument 'inV' must not be NULL!");
		ChronoEdgeImpl edge = new ChronoEdgeImpl(id, outV, label, inV);
		edge.updateLifecycleStatus(ElementLifecycleStatus.NEW);
		return edge;
	}

	public static ChronoEdgeImpl incomingEdgeFromRecord(final ChronoVertexImpl owner, final String label,
			final EdgeTargetRecord record) {
		checkNotNull(owner, "Precondition violation - argument 'owner' must not be NULL!");
		checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
		checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
		String id = record.getEdgeId();
		String otherEndVertexId = record.getOtherEndVertexId();
		ChronoEdgeImpl edge = new ChronoEdgeImpl(id, label, owner, otherEndVertexId, owner.id(),
				ElementLifecycleStatus.PERSISTED);
		// tell the edge that it's properties must be loaded from the backing data store on first access
		edge.lazyLoadProperties = true;
		return edge;
	}

	public static ChronoEdgeImpl outgoingEdgeFromRecord(final ChronoVertexImpl owner, final String label,
			final EdgeTargetRecord record) {
		checkNotNull(owner, "Precondition violation - argument 'owner' must not be NULL!");
		checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
		checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
		String id = record.getEdgeId();
		String otherEndVertexId = record.getOtherEndVertexId();
		ChronoEdgeImpl edge = new ChronoEdgeImpl(id, label, owner, owner.id(), otherEndVertexId,
				ElementLifecycleStatus.PERSISTED);
		// tell the edge that it's properties must be loaded from the backing data store on first access
		edge.lazyLoadProperties = true;
		return edge;
	}

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final String outVid;
	private final String inVid;
	protected final Map<String, ChronoProperty<?>> properties;

	private transient WeakReference<ChronoVertex> outVcache;
	private transient WeakReference<ChronoVertex> inVcache;
	private transient boolean lazyLoadProperties;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected ChronoEdgeImpl(final ChronoGraphInternal graph, final ChronoGraphTransactionInternal tx, final String id,
			final String outVid, final String label, final String inVid, final Set<PropertyRecord> properties) {
		super(graph, tx, id, label, false);
		checkNotNull(outVid, "Precondition violation - argument 'outVid' must not be NULL!");
		checkNotNull(inVid, "Precondition violation - argument 'inVid' must not be NULL!");
		this.outVid = outVid;
		this.inVid = inVid;
		this.properties = Maps.newHashMap();
		for (PropertyRecord pRecord : properties) {
			String propertyName = pRecord.getKey();
			Object propertyValue = pRecord.getValue();
			this.properties.put(propertyName, new ChronoProperty(this, propertyName, propertyValue));
		}
	}

	protected ChronoEdgeImpl(final String id, final String label, final ChronoVertexImpl owner, final String outVid,
			final String inVid, final ElementLifecycleStatus status) {
		super(owner.graph(), owner.getOwningTransaction(), id, label, false);
		checkNotNull(outVid, "Precondition violation - argument 'outVid' must not be NULL!");
		checkNotNull(inVid, "Precondition violation - argument 'inVid' must not be NULL!");
		checkNotNull(owner, "Precondition violation - argument 'owner' must not be NULL!");
		this.outVid = outVid;
		this.inVid = inVid;
		this.properties = Maps.newHashMap();
		if (owner.id().equals(outVid)) {
			this.outVcache = new WeakReference<>(owner);
		} else if (owner.id().equals(inVid)) {
			this.inVcache = new WeakReference<>(owner);
		} else {
			throw new IllegalArgumentException("The given owner is neither the in-vertex nor the out-vertex!");
		}
		this.updateLifecycleStatus(status);
	}

	protected ChronoEdgeImpl(final String id, final ChronoVertexImpl outV, final String label,
			final ChronoVertexImpl inV) {
		super(inV.graph(), inV.getGraphTransaction(), id, label, false);
		checkNotNull(outV, "Precondition violation - argument 'outV' must not be NULL!");
		checkNotNull(inV, "Precondition violation - argument 'inV' must not be NULL!");
		// we need to check the access on the vertices, as we might have to transition them to another transaction
		outV.checkAccess();
		inV.checkAccess();
		if (outV.getOwningTransaction().equals(inV.getOwningTransaction()) == false) {
			throw new IllegalArgumentException("The given vertices are bound to different transactions!");
		}
		if (outV.getOwningThread().equals(Thread.currentThread()) == false
				|| inV.getOwningThread().equals(Thread.currentThread()) == false) {
			throw new IllegalStateException("Cannot create edge - neighboring vertices belong to different threads!");
		}
		this.outVid = outV.id();
		this.inVid = inV.id();
		this.properties = Maps.newHashMap();
		this.inVcache = new WeakReference<>(inV);
		this.outVcache = new WeakReference<>(outV);
	}

	// =================================================================================================================
	// TINKERPOP 3 API
	// =================================================================================================================

	@Override
	public Iterator<Vertex> vertices(final Direction direction) {
		this.checkAccess();
		checkNotNull(direction, "Precondition violation - argument 'direction' must not be NULL!");
		switch (direction) {
		case BOTH:
			// note: according to gremlin specification, the out vertex should be returned first.
			return Iterators.forArray(this.outVertex(), this.inVertex());
		case IN:
			return Iterators.forArray(this.inVertex());
		case OUT:
			return Iterators.forArray(this.outVertex());
		default:
			throw new IllegalArgumentException("Unkown 'Direction' literal: " + direction);
		}
	}

	@Override
	public <V> ChronoProperty<V> property(final String key, final V value) {
		ElementHelper.validateProperty(key, value);
		this.checkAccess();
		this.loadLazyPropertiesIfRequired();
		this.logPropertyChange(key, value);
		boolean exists = this.property(key).isPresent();
		ChronoProperty<V> newProperty = new ChronoProperty<>(this, key, value);
		if (exists) {
			this.changePropertyStatus(key, PropertyStatus.MODIFIED);
		} else {
			this.changePropertyStatus(key, PropertyStatus.NEW);
		}
		this.properties.put(key, newProperty);
		this.updateLifecycleStatus(ElementLifecycleStatus.PROPERTY_CHANGED);
		return newProperty;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
		this.checkAccess();
		this.loadLazyPropertiesIfRequired();
		Set<Property> matchingProperties = Sets.newHashSet();
		if (propertyKeys == null || propertyKeys.length <= 0) {
			// note: the TinkerPop test suite explicitly demands that predefined property keys,
			// such as T.id, T.label etc. are EXCLUDED from the iterator in this case.
			matchingProperties.addAll(this.properties.values());
		} else {
			for (String key : propertyKeys) {
				PredefinedProperty<?> predefinedProperty = ChronoGraphElementUtil.asPredefinedProperty(this, key);
				if (predefinedProperty != null) {
					matchingProperties.add(predefinedProperty);
				}
				Property property = this.properties.get(key);
				if (property != null) {
					matchingProperties.add(property);
				}
			}
		}
		return new PropertiesIterator<>(matchingProperties.iterator());
	}

	@Override
	public ChronoVertex inVertex() {
		this.checkAccess();
		ChronoVertex inVertex = null;
		if (this.inVcache != null) {
			// we have loaded this element once before, see if it's cached
			inVertex = this.inVcache.get();
		}
		if (inVertex == null) {
			// either we have never loaded this element before, or the garbage collector
			// decided to remove our cached instance. In this case, we need to reload it.
			inVertex = this.resolveVertex(this.inVid);
			// remember it in the cache
			this.inVcache = new WeakReference<>(inVertex);
		}
		return inVertex;
	}

	@Override
	public ChronoVertex outVertex() {
		this.checkAccess();
		ChronoVertex outVertex = null;
		if (this.outVcache != null) {
			// we have loaded this element once before, see if it's cached
			outVertex = this.outVcache.get();
		}
		if (outVertex == null) {
			// either we have never loaded this element before, or the garbage collector
			// decided to remove our cached instance. In this case, we need to reload it.
			outVertex = this.resolveVertex(this.outVid);
			// remember it in the cache
			this.outVcache = new WeakReference<>(outVertex);
		}
		return outVertex;
	}

	@Override
	public void remove() {
		this.checkAccess();
		this.logEdgeRemove();
		this.withoutRemovedCheck(() -> {
			super.remove();
			if (this.inVertex().equals(this.outVertex())) {
				// self reference, sufficient to call remove() only on one end
				ChronoProxyUtil.resolveVertexProxy(this.inVertex()).removeEdge(this);
			} else {
				// reference to other vertex, need to remove edge from both ends
				ChronoProxyUtil.resolveVertexProxy(this.inVertex()).removeEdge(this);
				ChronoProxyUtil.resolveVertexProxy(this.outVertex()).removeEdge(this);
			}
		});
	}

	@Override
	public String toString() {
		return StringFactory.edgeString(this);
	}

	@Override
	protected void reloadFromDatabase() {
		// clear the inV and outV caches (but keep the ids, as the vertices connected to an edge can never change)
		ElementLifecycleStatus[] nextStatus = new ElementLifecycleStatus[1];
		this.withoutRemovedCheck(() -> {
			this.withoutModificationCheck(() -> {
				this.inVcache = null;
				this.outVcache = null;
				this.properties.clear();
				ChronoDBTransaction backendTx = this.getOwningTransaction().getBackingDBTransaction();
				EdgeRecord eRecord = backendTx.get(ChronoGraphConstants.KEYSPACE_EDGE, this.id().toString());
				if (eRecord == null) {
					// edge was removed
					nextStatus[0] = ElementLifecycleStatus.REMOVED;
				} else {
					nextStatus[0] = ElementLifecycleStatus.PERSISTED;
					// load the properties
					this.lazyLoadProperties = false;
					for (PropertyRecord propertyRecord : eRecord.getProperties()) {
						String key = propertyRecord.getKey();
						Object value = propertyRecord.getValue();
						this.property(key, value);
					}
				}
				this.getTransactionContext().registerLoadedEdge(this);
			});
		});
		this.updateLifecycleStatus(nextStatus[0]);
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	@Override
	public void removeProperty(final String key) {
		this.checkAccess();
		this.loadLazyPropertiesIfRequired();
		this.logPropertyRemove(key);
		this.properties.remove(key);
		this.changePropertyStatus(key, PropertyStatus.REMOVED);
		this.updateLifecycleStatus(ElementLifecycleStatus.PROPERTY_CHANGED);
	}

	public EdgeRecord toRecord() {
		String id = this.id();
		String label = this.label();
		this.loadLazyPropertiesIfRequired();
		return new EdgeRecord(id, this.outVid, label, this.inVid, this.properties);
	}

	@Override
	public void updateLifecycleStatus(final ElementLifecycleStatus status) {
		super.updateLifecycleStatus(status);
		if (this.isModificationCheckActive()) {
			if (status == ElementLifecycleStatus.NEW || status == ElementLifecycleStatus.REMOVED
					|| status == ElementLifecycleStatus.OBSOLETE) {
				// need to update adjacency lists of in and out vertex
				this.inVertex().updateLifecycleStatus(ElementLifecycleStatus.EDGE_CHANGED);
				this.outVertex().updateLifecycleStatus(ElementLifecycleStatus.EDGE_CHANGED);
			}
			if (status.isDirty()) {
				this.getTransactionContext().markEdgeAsModified(this);
			}
		}
	}

	// =====================================================================================================================
	// DEBUG OUTPUT
	// =====================================================================================================================

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void loadLazyPropertiesIfRequired() {
		if (this.lazyLoadProperties == false) {
			// lazy loading of properties is not required
			return;
		}
		ChronoGraphTransaction graphTx = this.getGraphTransaction();
		EdgeRecord edgeRecord = graphTx.getBackingDBTransaction().get(ChronoGraphConstants.KEYSPACE_EDGE, this.id());
		if (edgeRecord == null) {
			throw new IllegalStateException(
					"Failed to load edge properties - there is no backing Edge Record in the database for ID: '"
							+ this.id().toString() + "'!");
		}
		// load the properties from the edge record
		for (PropertyRecord propertyRecord : edgeRecord.getProperties()) {
			String key = propertyRecord.getKey();
			Object value = propertyRecord.getValue();
			ChronoProperty<?> property = new ChronoProperty(this, key, value, true);
			this.properties.put(key, property);
		}
		// remember that properties do not need to be re-loaded
		this.lazyLoadProperties = false;
	}

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
		messageBuilder.append(" on Edge ");
		messageBuilder.append(this.toString());
		messageBuilder.append(" (Object ID: ");
		messageBuilder.append(System.identityHashCode(this));
		messageBuilder.append(")");
		ChronoLogger.logTrace(messageBuilder.toString());
	}

	private void logEdgeRemove() {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
			// log level is higher than trace, no need to prepare the message
			return;
		}
		// prepare some debug output
		StringBuilder messageBuilder = new StringBuilder();
		messageBuilder.append("[GRAPH MODIFICATION] Removing Edge ");
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
		messageBuilder.append("' from Edge ");
		messageBuilder.append(this.toString());
		messageBuilder.append(" (Object ID: ");
		messageBuilder.append(System.identityHashCode(this));
		messageBuilder.append(")");
		ChronoLogger.logTrace(messageBuilder.toString());
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private class PropertiesIterator<V> implements Iterator<Property<V>> {

		@SuppressWarnings("rawtypes")
		private Iterator<Property> iter;

		@SuppressWarnings("rawtypes")
		public PropertiesIterator(final Iterator<Property> iter) {
			this.iter = iter;
		}

		@Override
		public boolean hasNext() {
			return this.iter.hasNext();
		}

		@Override
		@SuppressWarnings("unchecked")
		public Property<V> next() {
			Property<?> p = this.iter.next();
			return (Property<V>) p;
		}

	}

}
