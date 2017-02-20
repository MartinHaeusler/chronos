package org.chronos.chronograph.internal.impl.transaction;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.query.SearchSpecification;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoProperty;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.graph.proxy.ChronoEdgeProxy;
import org.chronos.chronograph.internal.impl.structure.graph.proxy.ChronoVertexProxy;
import org.chronos.chronograph.internal.impl.util.ChronoGraphQueryUtil;
import org.chronos.chronograph.internal.impl.util.IdentityWrapper;
import org.chronos.common.base.CCC;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.logging.LogLevel;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

/**
 * The {@link GraphTransactionContext} keeps track of the elements which have been modified by client code.
 *
 * <p>
 * A TransactionContext is always associated with a {@link ChronoGraphTransaction} that contains the context. The context may be cleared when a {@link ChronoGraphTransaction#rollback()} occurs on the owning transaction.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class GraphTransactionContext {

	private final SetMultimap<String, IdentityWrapper<ChronoProperty<?>>> propertyNameToModifiedProperties = HashMultimap
			.create();
	private final Map<String, ChronoVertexImpl> modifiedVertices = Maps.newHashMap();
	private final Map<String, ChronoEdgeImpl> modifiedEdges = Maps.newHashMap();

	private final Map<String, ChronoVertexImpl> loadedVertices = new MapMaker().weakValues().makeMap();
	private final Map<String, ChronoEdgeImpl> loadedEdges = new MapMaker().weakValues().makeMap();

	private final Map<String, ChronoVertexProxy> idToVertexProxy = new MapMaker().weakValues().makeMap();
	private final Map<String, ChronoEdgeProxy> idToEdgeProxy = new MapMaker().weakValues().makeMap();

	private final Map<String, Object> modifiedVariables = Maps.newHashMap();

	// =====================================================================================================================
	// LOADED ELEMENT CACHE API
	// =====================================================================================================================

	public ChronoVertexImpl getLoadedVertexForId(final String id) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		return this.loadedVertices.get(id);
	}

	public void registerLoadedVertex(final ChronoVertexImpl vertex) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		this.loadedVertices.put(vertex.id(), vertex);
	}

	public ChronoEdgeImpl getLoadedEdgeForId(final String id) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		return this.loadedEdges.get(id);
	}

	public void registerLoadedEdge(final ChronoEdgeImpl edge) {
		checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
		this.loadedEdges.put(edge.id(), edge);
	}

	public void registerVertexProxyInCache(final ChronoVertexProxy proxy) {
		checkNotNull(proxy, "Precondition violation - argument 'proxy' must not be NULL!");
		this.idToVertexProxy.put(proxy.id(), proxy);
	}

	public void registerEdgeProxyInCache(final ChronoEdgeProxy proxy) {
		checkNotNull(proxy, "Precondition violation - argument 'proxy' must not be NULL!");
		this.idToEdgeProxy.put(proxy.id(), proxy);
	}

	/**
	 * Returns the {@link ChronoVertexProxy Vertex Proxy} for the {@link Vertex} with the given ID.
	 *
	 * <p>
	 * Bear in mind that proxies are stored in a weak fashion, i.e. they may be garbage collected if they are not referenced anywhere else. This method may and <b>will</b> return <code>null</code> in such cases. Think of this method as a cache.
	 *
	 * <p>
	 * To reliably get a proxy, use {@link #getOrCreateVertexProxy(Vertex)} instead.
	 *
	 * @param vertexId
	 *            The ID of the vertex to get the cached proxy for. Must not be <code>null</code>.
	 * @return The vertex proxy for the given vertex ID, or <code>null</code> if none is cached.
	 */
	private ChronoVertexProxy getWeaklyCachedVertexProxy(final String vertexId) {
		checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
		return this.idToVertexProxy.get(vertexId);
	}

	public ChronoVertexProxy getOrCreateVertexProxy(final Vertex vertex) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		if (vertex instanceof ChronoVertexProxy) {
			// already is a proxy
			return (ChronoVertexProxy) vertex;
		}
		// check if we have a proxy
		ChronoVertexProxy proxy = this.getWeaklyCachedVertexProxy((String) vertex.id());
		if (proxy != null) {
			// we already have a proxy; reuse it
			return proxy;
		}
		// we don't have a proxy yet; create one
		proxy = new ChronoVertexProxy((ChronoVertexImpl) vertex);
		this.registerVertexProxyInCache(proxy);
		return proxy;
	}

	/**
	 * Returns the {@link ChronoEdgeProxy Edge Proxy} for the {@link Edge} with the given ID.
	 *
	 * <p>
	 * Bear in mind that proxies are stored in a weak fashion, i.e. they may be garbage collected if they are not referenced anywhere else. This method may and <b>will</b> return <code>null</code> in such cases. Think of this method as a cache.
	 *
	 * <p>
	 * To reliably get a proxy, use {@link #getOrCreateEdgeProxy(Edge)} instead.
	 *
	 * @param edgeId
	 *            The ID of the vertex to get the cached proxy for. Must not be <code>null</code>.
	 * @return The edge proxy for the given vertex ID, or <code>null</code> if none is cached.
	 */
	private ChronoEdgeProxy getWeaklyCachedEdgeProxy(final String edgeId) {
		checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
		return this.idToEdgeProxy.get(edgeId);
	}

	public ChronoEdgeProxy getOrCreateEdgeProxy(final Edge edge) {
		checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
		if (edge instanceof ChronoEdgeProxy) {
			// already is a proxy
			return (ChronoEdgeProxy) edge;
		}
		// check if we have a proxy
		ChronoEdgeProxy proxy = this.getWeaklyCachedEdgeProxy((String) edge.id());
		// we don't have a proxy yet; create one
		proxy = new ChronoEdgeProxy((ChronoEdgeImpl) edge);
		this.registerEdgeProxyInCache(proxy);
		return proxy;
	}

	// =====================================================================================================================
	// MODIFICATION (IS-DIRTY) API
	// =====================================================================================================================

	public Set<ChronoVertexImpl> getModifiedVertices() {
		return Collections.unmodifiableSet(Sets.newHashSet(this.modifiedVertices.values()));
	}

	public Set<ChronoEdgeImpl> getModifiedEdges() {
		return Collections.unmodifiableSet(Sets.newHashSet(this.modifiedEdges.values()));
	}

	public Set<String> getModifiedVariables() {
		return Collections.unmodifiableSet(Sets.newHashSet(this.modifiedVariables.keySet()));
	}

	public Set<ChronoElement> getModifiedElements() {
		return Sets.union(this.getModifiedVertices(), this.getModifiedEdges());
	}

	public boolean isDirty() {
		return this.modifiedVertices.isEmpty() == false || this.modifiedEdges.isEmpty() == false
				|| this.propertyNameToModifiedProperties.isEmpty() == false
				|| this.modifiedVariables.isEmpty() == false;
	}

	public boolean isVertexModified(final ChronoVertexImpl vertex) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		return this.modifiedVertices.containsKey(vertex.id());
	}

	public boolean isVertexModified(final String vertexId) {
		checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
		return this.modifiedVertices.containsKey(vertexId);
	}

	public void markVertexAsModified(final ChronoVertexImpl vertex) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		this.logMarkVertexAsModified(vertex);
		this.modifiedVertices.put(vertex.id(), vertex);
	}

	public boolean isEdgeModified(final ChronoEdgeImpl edge) {
		checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
		return this.modifiedEdges.containsKey(edge.id());
	}

	public boolean isEdgeModified(final String edgeId) {
		checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
		return this.modifiedEdges.containsKey(edgeId);
	}

	public void markEdgeAsModified(final ChronoEdgeImpl edge) {
		checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
		this.logMarkEdgeAsModified(edge);
		this.modifiedEdges.put(edge.id(), edge);
	}

	public void markPropertyAsModified(final ChronoProperty<?> property, final Object previousValue) {
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		this.logMarkPropertyAsModified(property);
		// remove the old entry (if any)
		this.propertyNameToModifiedProperties.remove(property.key(), IdentityWrapper.of(property));
		// add an entry for the new value
		this.propertyNameToModifiedProperties.put(property.key(), IdentityWrapper.of(property));
	}

	public boolean isPropertyModified(final ChronoProperty<?> property) {
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		return this.propertyNameToModifiedProperties.containsValue(IdentityWrapper.of(property));
	}

	public List<ChronoProperty<?>> getModifiedProperties(final SearchSpecification searchSpec) {
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		// only look at the modified properties that have the property key we are interested in
		String propertyKey = searchSpec.getProperty();
		Collection<IdentityWrapper<ChronoProperty<?>>> modifiedProperties = this.propertyNameToModifiedProperties
				.get(propertyKey);
		// use a list here, because #hashCode and #equals on Properties is defined on key and value only (not on parent
		// element id)
		List<ChronoProperty<?>> resultList = Lists.newArrayList();
		String searchText = searchSpec.getSearchText();
		Condition condition = searchSpec.getCondition();
		TextMatchMode matchMode = searchSpec.getMatchMode();
		for (IdentityWrapper<ChronoProperty<?>> wrapper : modifiedProperties) {
			ChronoProperty<?> property = wrapper.get();
			if (property.isRemoved()) {
				continue;
			}
			if (property.key().equals(searchSpec.getProperty()) == false) {
				continue;
			}
			Object value = property.value();
			if (ChronoGraphQueryUtil.conditionApplies(condition, value, searchText, matchMode)) {
				resultList.add(property);
			}
		}
		return resultList;
	}

	public boolean isVariableModified(final String variableName) {
		checkNotNull(variableName, "Precondition violation - argument 'variableName' must not be NULL!");
		return this.modifiedVariables.containsKey(variableName);
	}

	public boolean isVariableRemoved(final String variableName) {
		checkNotNull(variableName, "Precondition violation - argument 'variableName' must not be NULL!");
		return this.modifiedVariables.containsKey(variableName) && this.modifiedVariables.get(variableName) == null;
	}

	public Object getModifiedVariableValue(final String variableName) {
		checkNotNull(variableName, "Precondition violation - argument 'variableName' must not be NULL!");
		if (this.modifiedVariables.containsKey(variableName)) {
			return this.modifiedVariables.get(variableName);
		} else {
			return null;
		}
	}

	// =====================================================================================================================
	// GRAPH VARIABLES API
	// =====================================================================================================================

	public void removeVariable(final String variableName) {
		checkNotNull(variableName, "Precondition violation - argument 'variableName' must not be NULL!");
		this.modifiedVariables.put(variableName, null);
	}

	public void setVariableValue(final String variableName, final Object value) {
		checkNotNull(variableName, "Precondition violation - argument 'variableName' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		this.modifiedVariables.put(variableName, value);
	}

	public Set<String> getRemovedVariables() {
		Set<String> set = this.getModifiedVariables().stream().filter(vName -> this.isVariableRemoved(vName))
				.collect(Collectors.toSet());
		return Collections.unmodifiableSet(set);
	}

	public void clear() {
		this.modifiedVertices.clear();
		this.modifiedEdges.clear();
		this.propertyNameToModifiedProperties.clear();
		this.modifiedVariables.clear();
		this.loadedVertices.clear();
		this.loadedEdges.clear();
		this.idToVertexProxy.clear();
		this.idToEdgeProxy.clear();
	}

	public ChronoVertexImpl getModifiedVertex(final String id) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		return this.modifiedVertices.get(id);
	}

	public ChronoEdgeImpl getModifiedEdge(final String id) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		return this.modifiedEdges.get(id);
	}

	// =====================================================================================================================
	// DEBUG LOGGING
	// =====================================================================================================================

	private void logMarkVertexAsModified(final ChronoVertex vertex) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
			// log level is higher than trace, no need to prepare the message
			return;
		}
		// prepare some debug output
		StringBuilder messageBuilder = new StringBuilder();
		messageBuilder.append("[GRAPH MODIFICATION] Marking Vertex as modified: ");
		messageBuilder.append(vertex.toString());
		messageBuilder.append(" (Object ID: '");
		messageBuilder.append(System.identityHashCode(vertex));
		messageBuilder.append("). ");
		if (this.modifiedVertices.containsKey(vertex.id())) {
			messageBuilder.append("This Vertex was already marked as modified in this transaction.");
		} else {
			messageBuilder.append("This Vertex has not yet been marked as modified in this transaction.");
		}
		ChronoLogger.logTrace(messageBuilder.toString());
	}

	private void logMarkEdgeAsModified(final ChronoEdge edge) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
			// log level is higher than trace, no need to prepare the message
			return;
		}
		// prepare some debug output
		StringBuilder messageBuilder = new StringBuilder();
		messageBuilder.append("[GRAPH MODIFICATION] Marking Edge as modified: ");
		messageBuilder.append(edge.toString());
		messageBuilder.append(" (Object ID: '");
		messageBuilder.append(System.identityHashCode(edge));
		messageBuilder.append("). ");
		if (this.modifiedEdges.containsKey(edge.id())) {
			messageBuilder.append("This Edge was already marked as modified in this transaction.");
		} else {
			messageBuilder.append("This Edge has not yet been marked as modified in this transaction.");
		}
		ChronoLogger.logTrace(messageBuilder.toString());
	}

	private void logMarkPropertyAsModified(final ChronoProperty<?> property) {
		if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
			// log level is higher than trace, no need to prepare the message
			return;
		}
		// prepare some debug output
		StringBuilder messageBuilder = new StringBuilder();
		messageBuilder.append("[GRAPH MODIFICATION] Marking Property as modified: ");
		messageBuilder.append(property.toString());
		messageBuilder.append(" (Object ID: '");
		messageBuilder.append(System.identityHashCode(property));
		messageBuilder.append("). ");
		if (property.isPresent()) {
			ChronoElement owner = property.element();
			messageBuilder.append("Property is owned by: ");
			messageBuilder.append(owner.toString());
			messageBuilder.append(" (Object ID: ");
			messageBuilder.append(System.identityHashCode(owner));
			messageBuilder.append(")");
		}
		ChronoLogger.logTrace(messageBuilder.toString());
	}

}
