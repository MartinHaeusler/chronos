package org.chronos.chronograph.internal.impl.structure.graph;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronograph.api.exceptions.GraphInvariantViolationException;
import org.chronos.chronograph.api.jmx.ChronoGraphTransactionStatistics;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.structure.PropertyStatus;
import org.chronos.chronograph.api.structure.record.IEdgeTargetRecord;
import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.chronograph.api.structure.record.IVertexPropertyRecord;
import org.chronos.chronograph.api.structure.record.IVertexRecord;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.api.transaction.ChronoGraphTransactionInternal;
import org.chronos.chronograph.internal.impl.structure.record.EdgeTargetRecordWithLabel;
import org.chronos.chronograph.internal.impl.structure.record2.EdgeTargetRecord2;
import org.chronos.chronograph.internal.impl.structure.record3.VertexRecord3;
import org.chronos.chronograph.internal.impl.util.ChronoGraphElementUtil;
import org.chronos.chronograph.internal.impl.util.ChronoGraphLoggingUtil;
import org.chronos.chronograph.internal.impl.util.ChronoId;
import org.chronos.chronograph.internal.impl.util.ChronoProxyUtil;
import org.chronos.chronograph.internal.impl.util.PredefinedVertexProperty;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.*;
import static org.chronos.common.logging.ChronosLogMarker.*;

public class ChronoVertexImpl extends AbstractChronoElement implements Vertex, ChronoVertex {

    private static final Logger log = LoggerFactory.getLogger(ChronoVertexImpl.class);

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private SetMultimap<String, ChronoEdge> labelToIncomingEdges = null;
    private SetMultimap<String, ChronoEdge> labelToOutgoingEdges = null;
    private Map<String, ChronoVertexProperty<?>> properties = null;

    protected Reference<IVertexRecord> recordReference;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public ChronoVertexImpl(final String id, final ChronoGraphInternal g, final ChronoGraphTransactionInternal tx,
                            final String label) {
        this(g, tx, id, label);
    }

    public ChronoVertexImpl(final ChronoGraphInternal g, final ChronoGraphTransactionInternal tx,
                            final IVertexRecord record) {
        super(g, tx, record.getId(), record.getLabel());
        this.recordReference = new WeakReference<>(record);
        this.updateLifecycleStatus(ElementLifecycleEvent.RELOADED_FROM_DB_AND_IN_SYNC);
    }

    public ChronoVertexImpl(final ChronoGraphInternal g, final ChronoGraphTransactionInternal tx, final String id,
                            final String label) {
        super(g, tx, id, label);
        this.labelToOutgoingEdges = HashMultimap.create();
        this.labelToIncomingEdges = HashMultimap.create();
        this.properties = Maps.newHashMap();
        this.recordReference = null;
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
    public <V> VertexProperty<V> property(final String key, final V value) {
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        this.checkAccess();
        this.ensureVertexRecordIsLoaded();
        if (value == null) {
            // Since Gremlin 3.5.2: setting a property to value NULL removes it.
            this.removeProperty(key);
            return VertexProperty.empty();
        }

        this.logPropertyChange(key, value);
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        ChronoVertexProperty<V> property = (ChronoVertexProperty<V>) this.properties.get(key);
        if (property == null) {
            property = new ChronoVertexProperty<>(this, key, value);
            this.changePropertyStatus(key, PropertyStatus.NEW);
            this.getTransactionContext().markPropertyAsModified(property);
            this.properties.put(key, property);
        } else {
            property.set(value);
            this.changePropertyStatus(key, PropertyStatus.MODIFIED);
        }
        this.updateLifecycleStatus(ElementLifecycleEvent.PROPERTY_CHANGED);
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
        this.ensureVertexRecordIsLoaded();
        if (id == null) {
            id = ChronoId.random();
            // we generated the ID ourselves, it did not come from the user
            userProvidedId = false;
        }
        String edgeId = (String) id;
        this.graph.tx().readWrite();
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
        inV.ensureVertexRecordIsLoaded();
        inV.labelToIncomingEdges.put(label, edge);
        this.updateLifecycleStatus(ElementLifecycleEvent.ADJACENT_EDGE_ADDED_OR_REMOVED);
        inV.updateLifecycleStatus(ElementLifecycleEvent.ADJACENT_EDGE_ADDED_OR_REMOVED);
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
        if (id != null) {
            // user-supplied ids are not supported
            throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();
        }
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
        this.ensureVertexRecordIsLoaded();
        this.logPropertyChange(key, value);
        ChronoVertexProperty<V> property = new ChronoVertexProperty<>(this, key, value);
        ElementHelper.attachProperties(property, keyValues);
        if (this.property(key).isPresent()) {
            this.changePropertyStatus(key, PropertyStatus.MODIFIED);
        } else {
            this.changePropertyStatus(key, PropertyStatus.NEW);
        }
        this.properties.put(key, property);
        this.getTransactionContext().markPropertyAsModified(property);
        this.updateLifecycleStatus(ElementLifecycleEvent.PROPERTY_CHANGED);
        return property;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        this.checkAccess();
        if (this.recordReference != null) {
            // we're in read-only mode
            return this.edgesFromLazyVertex(direction, edgeLabels);
        } else {
            // we're in read-write mode
            return this.edgesFromLoadedVertex(direction, edgeLabels);
        }
    }

    private Iterator<Edge> edgesFromLazyVertex(final Direction direction, String... edgeLabels) {
        IVertexRecord vertexRecord = this.getRecord();
        switch (direction) {
            case BOTH:
                // note that we do NOT want self-edges (e.g. v1->v1) to appear twice. Therefore, we use
                // a set to eliminate duplicates. Furthermore, Gremlin wants ot have out-edges before in-edges
                // in the iterator, so we use concatenated streams to ensure this. Furthermore,
                // "record.getOutgoingEdges(labels)" and "record.getIncomingEdges(labels)" will return ALL
                // of their respective edges if the "edgeLabels" parameter is NULL or empty, which is in line
                // with the gremlin specification.
                List<EdgeTargetRecordWithLabel> outgoingEdges = vertexRecord.getOutgoingEdges(edgeLabels);
                List<EdgeTargetRecordWithLabel> incomingEdges = vertexRecord.getIncomingEdges(edgeLabels);
                return Stream.concat(
                    outgoingEdges.stream().map(this::loadOutgoingEdgeTargetRecord),
                    incomingEdges.stream().map(this::loadIncomingEdgeTargetRecord)
                ).iterator();
            case IN:
                return Iterators.transform(vertexRecord.getIncomingEdges(edgeLabels).iterator(), this::loadIncomingEdgeTargetRecord);
            case OUT:
                return Iterators.transform(vertexRecord.getOutgoingEdges(edgeLabels).iterator(), this::loadOutgoingEdgeTargetRecord);
            default:
                throw new UnknownEnumLiteralException(direction);
        }

    }

    private Iterator edgesFromLoadedVertex(final Direction direction, String... edgeLabels) {
        switch (direction) {
            case BOTH:
                if (edgeLabels == null || edgeLabels.length <= 0) {
                    // return all
                    // note that we wrap the internal collections in new hash sets; gremlin specification states
                    // that no concurrent modification excpetions should ever be thrown when iterating over edges
                    // in a single-threaded program.

                    int expectedSize = this.labelToOutgoingEdges.size() + this.labelToIncomingEdges.size();
                    List<Edge> edges = Lists.newArrayListWithExpectedSize(expectedSize);
                    // Gremlin wants ot have out-edges before in-edges in the iterator.
                    edges.addAll(this.labelToOutgoingEdges.values());
                    edges.addAll(this.labelToIncomingEdges.values());
                    return edges.iterator();
                } else {
                    // return the ones with matching labels
                    List<Edge> edges = Stream.of(edgeLabels).distinct().flatMap(edgeLabel ->
                        Streams.concat(
                            this.labelToOutgoingEdges.get(edgeLabel).stream(),
                            this.labelToIncomingEdges.get(edgeLabel).stream()
                        )
                    ).collect(Collectors.toList());
                    return edges.iterator();
                }
            case IN:
                if (edgeLabels == null || edgeLabels.length <= 0) {
                    // return all
                    // note that we wrap the internal collections in new hash sets; gremlin specification states
                    // that no concurrent modification exceptions should ever be thrown when iterating over edges
                    // in a single-threaded program.
                    return Lists.newArrayList(this.labelToIncomingEdges.values()).iterator();
                } else {
                    // return the ones with matching labels
                    List<Edge> list = Stream.of(edgeLabels)
                        .flatMap(label -> this.labelToIncomingEdges.get(label).stream())
                        .collect(Collectors.toList());
                    return list.iterator();
                }
            case OUT:
                if (edgeLabels == null || edgeLabels.length <= 0) {
                    // return all
                    // note that we wrap the internal collections in new hash sets; gremlin specification states
                    // that no concurrent modification exceptions should ever be thrown when iterating over edges
                    // in a single-threaded program.
                    return Lists.newArrayList(this.labelToOutgoingEdges.values()).iterator();
                } else {
                    // return the ones with matching labels
                    List<Edge> list = Stream.of(edgeLabels)
                        .flatMap(label -> this.labelToOutgoingEdges.get(label).stream())
                        .collect(Collectors.toList());
                    return list.iterator();
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
    public <V> V value(final String key) {
        this.checkAccess();
        // note: this is more efficient than the standard implementation in TinkerPop
        // because it avoids the creation of an iterator via #properties(key).
        return this.<V>property(key).value();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        this.checkAccess();
        // Since Gremlin 3.5.2: querying properties(null) is now allowed and returns the empty iterator.
        if (propertyKeys != null && propertyKeys.length > 0 && Arrays.stream(propertyKeys).allMatch(java.util.Objects::isNull)) {
            return Collections.emptyIterator();
        }
        if (propertyKeys == null || propertyKeys.length <= 0) {
            if (this.recordReference == null) {
                // we are eagerly loaded
                if (this.properties == null) {
                    // we have no properties
                    return Collections.emptyIterator();
                } else {
                    // use all existing properties. Copy the set to prevent concurrent modification exception,
                    // as defined by the gremlin standard.
                    return (Iterator) (Sets.newHashSet(this.properties.values())).iterator();
                }
            } else {
                // we are lazily loaded; use all the properties prescribed by our record,
                // and resolve them one by one as needed
                IVertexRecord vertexRecord = this.getRecord();
                return (Iterator) vertexRecord.getProperties().stream().map(IPropertyRecord::getKey).map(this::getSingleProperty).iterator();
            }
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
        this.ensureVertexRecordIsLoaded();
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

    protected Edge loadOutgoingEdgeTargetRecord(EdgeTargetRecordWithLabel recordWithLabel) {
        checkNotNull(recordWithLabel, "Precondition violation - argument 'recordWithLabel' must not be NULL!");
        String label = recordWithLabel.getLabel();
        IEdgeTargetRecord record;
        if (recordWithLabel.getRecord() instanceof EdgeTargetRecord2) {
            // it's an internal object, use it directly
            record = recordWithLabel.getRecord();
        } else {
            // we got this somehow from the user, copy it
            record = new EdgeTargetRecord2(recordWithLabel.getRecord().getEdgeId(), recordWithLabel.getRecord().getOtherEndVertexId());
        }
        return this.owningTransaction.loadOutgoingEdgeFromEdgeTargetRecord(this, label, record);
    }

    protected Edge loadIncomingEdgeTargetRecord(EdgeTargetRecordWithLabel recordWithLabel) {
        checkNotNull(recordWithLabel, "Precondition violation - argument 'recordWithLabel' must not be NULL!");
        String label = recordWithLabel.getLabel();
        IEdgeTargetRecord record;
        if (recordWithLabel.getRecord() instanceof EdgeTargetRecord2) {
            // it's an internal object, use it directly
            record = recordWithLabel.getRecord();
        } else {
            // we got this somehow from the user, copy it
            record = new EdgeTargetRecord2(recordWithLabel.getRecord().getEdgeId(), recordWithLabel.getRecord().getOtherEndVertexId());
        }
        return this.owningTransaction.loadIncomingEdgeFromEdgeTargetRecord(this, label, record);
    }

    protected void loadRecordContents() {
        if (this.labelToIncomingEdges == null) {
            this.labelToIncomingEdges = HashMultimap.create();
        }
        if (this.labelToOutgoingEdges == null) {
            this.labelToOutgoingEdges = HashMultimap.create();
        }
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        IVertexRecord vertexRecord = this.getRecord();
        this.label = vertexRecord.getLabel();
        for (IVertexPropertyRecord pRecord : vertexRecord.getProperties()) {
            // do not overwrite already loaded properties
            if (this.properties.containsKey(pRecord.getKey()) == false) {
                ChronoVertexProperty<?> property = loadPropertyRecord(pRecord);
                this.properties.put(property.key(), property);
            }
        }
        for (Entry<String, IEdgeTargetRecord> entry : vertexRecord.getIncomingEdgesByLabel().entries()) {
            String label = entry.getKey();
            IEdgeTargetRecord eRecord = entry.getValue();
            ChronoEdge edge = this.owningTransaction.loadIncomingEdgeFromEdgeTargetRecord(this, label, eRecord);
            this.labelToIncomingEdges.put(edge.label(), edge);
        }
        for (Entry<String, IEdgeTargetRecord> entry : vertexRecord.getOutgoingEdgesByLabel().entries()) {
            String label = entry.getKey();
            IEdgeTargetRecord eRecord = entry.getValue();
            ChronoEdge edge = this.owningTransaction.loadOutgoingEdgeFromEdgeTargetRecord(this, label, eRecord);
            this.labelToOutgoingEdges.put(edge.label(), edge);
        }
        this.recordReference = null;
    }

    private ChronoVertexProperty<?> loadPropertyRecord(final IVertexPropertyRecord pRecord) {
        ChronoVertexProperty<?> property = new ChronoVertexProperty<>(this, pRecord.getKey(), pRecord.getValue());
        for (Entry<String, IPropertyRecord> pEntry : pRecord.getProperties().entrySet()) {
            String metaKey = pEntry.getKey();
            IPropertyRecord metaProperty = pEntry.getValue();
            property.property(metaKey, metaProperty.getValue(), true);
        }
        return property;
    }

    @Override
    public void removeProperty(final String key) {
        this.checkAccess();
        this.logPropertyRemove(key);
        this.ensureVertexRecordIsLoaded();
        this.properties.remove(key);
        this.changePropertyStatus(key, PropertyStatus.REMOVED);
        this.updateLifecycleStatus(ElementLifecycleEvent.PROPERTY_CHANGED);
    }

    @Override
    public void notifyPropertyChanged(final ChronoProperty<?> chronoProperty) {
        if (chronoProperty instanceof ChronoVertexProperty == false) {
            throw new IllegalArgumentException("Only VertexProperties can reside on a Vertex!");
        }
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        this.ensureVertexRecordIsLoaded();
        ChronoVertexProperty<?> existingProperty = this.properties.get(chronoProperty.key());
        if (existingProperty != null && existingProperty != chronoProperty) {
            throw new IllegalStateException("Multiple instances of same vertex property detected. Key is '" + chronoProperty.key() + "'.");
        }
        this.properties.put(chronoProperty.key(), (ChronoVertexProperty) chronoProperty);
        this.updateLifecycleStatus(ElementLifecycleEvent.PROPERTY_CHANGED);
        this.changePropertyStatus(chronoProperty.key(), PropertyStatus.MODIFIED);
    }

    public IVertexRecord toRecord() {
        this.checkAccess();
        String id = this.id();
        String label = this.label();
        return new VertexRecord3(
            id, label,
            this.labelToIncomingEdges, this.labelToOutgoingEdges,
            this.properties);
    }

    @Override
    public void updateLifecycleStatus(final ElementLifecycleEvent event) {
        super.updateLifecycleStatus(event);
        if (this.isModificationCheckActive()) {
            if (this.getStatus().isDirty()) {
                this.getTransactionContext().markVertexAsModified(this);
            }
        }
    }

    @Override
    public void validateGraphInvariant() {
        this.withoutRemovedCheck(() -> {
            switch (this.getStatus()) {
                case PERSISTED:
                    /* fall throuhg*/
                case PROPERTY_CHANGED:
                    /* fall through*/
                case EDGE_CHANGED:
                    /* fall through*/
                case NEW:
                    // the vertex exists, check pointers from/to neighbor edges
                    this.edges(Direction.OUT).forEachRemaining(edge -> {
                        String label = edge.label();
                        ChronoEdge e = (ChronoEdge) edge;
                        if (e.isRemoved()) {
                            throw new GraphInvariantViolationException("The Vertex '" + this.id() + "' references the OUT Edge '" + e.id() + "' via Label '" + label + "', but this edge was removed!");
                        }
                        if (!Objects.equal(label, e.label())) {
                            throw new GraphInvariantViolationException("The Vertex '" + this.id() + "' references the OUT Edge '" + e.id() + "' via Label '" + label + "', but this edge has Label '" + e.label() + "'!");
                        }
                        if (!Objects.equal(e.outVertex(), this)) {
                            throw new GraphInvariantViolationException("The Vertex '" + this.id() + "' references the OUT Edge '" + e.id() + "' via Label '" + label + "', but the OUT Vertex of this Edge is different (v[" + e.outVertex().id() + "])!");
                        }
                    });
                    this.edges(Direction.IN).forEachRemaining(edge -> {
                        String label = edge.label();
                        ChronoEdge e = (ChronoEdge) edge;
                        if (e.isRemoved()) {
                            throw new GraphInvariantViolationException("The Vertex '" + this.id() + "' references the IN Edge '" + e.id() + "' via Label '" + label + "', but this edge was removed!");
                        }
                        if (!Objects.equal(label, e.label())) {
                            throw new GraphInvariantViolationException("The Vertex '" + this.id() + "' references the IN Edge '" + e.id() + "' via Label '" + label + "', but this edge has Label '" + e.label() + "'!");
                        }
                        if (!Objects.equal(e.inVertex(), this)) {
                            throw new GraphInvariantViolationException("The Vertex '" + this.id() + "' references the IN Edge '" + e.id() + "' via Label '" + label + "', but the IN Vertex of this Edge is different (v[" + e.outVertex().id() + "])!");
                        }
                    });
                    break;
                case OBSOLETE:
                    /* fall through*/
                case REMOVED:
                    // the vertex has been removed, check that neighbors are removed too
                    this.edges(Direction.BOTH).forEachRemaining(e -> {
                        if (!((ChronoEdge) e).isRemoved()) {
                            // neighboring edge is not deleted!
                            throw new GraphInvariantViolationException("The Vertex '" + this.id() + "' was deleted but its adjacent Edge '" + e.id() + "' still exists!");
                        }
                    });
                    break;
                default:
                    throw new UnknownEnumLiteralException(this.getStatus());
            }
        });
    }

    public void removeEdge(final ChronoEdgeImpl chronoEdge) {
        checkNotNull(chronoEdge, "Precondition violation - argument 'chronoEdge' must not be NULL!");
        this.checkAccess();
        this.ensureVertexRecordIsLoaded();
        boolean changed = false;
        if (chronoEdge.inVertex().equals(this)) {
            // incoming edge
            // remove whatever edge representation has been there with this edge-id
            boolean removed = this.labelToIncomingEdges.remove(chronoEdge.label(), chronoEdge);
            if (removed == false) {
                var message = new StringBuilder();
                message.append("Graph is inconsistent: failed to remove edge '");
                message.append(chronoEdge.id);
                message.append("' (Label: '");
                message.append(chronoEdge.label);
                message.append("') from its in-vertex '");
                message.append(this.id);
                message.append("' (Label: '");
                message.append(this.label);
                message.append("')!");
                throw new IllegalStateException(message.toString());
            }
            changed = true;
        }
        // note: this vertex can be in AND out vertex (self-edge!)
        if (chronoEdge.outVertex().equals(this)) {
            // outgoing edge
            // remove whatever edge representation has been there with this edge-id
            boolean removed = this.labelToOutgoingEdges.remove(chronoEdge.label(), chronoEdge);
            if (removed == false) {
                var message = new StringBuilder();
                message.append("Graph is inconsistent: failed to remove edge '");
                message.append(chronoEdge.id);
                message.append("' (Label: '");
                message.append(chronoEdge.label);
                message.append("') from its out-vertex '");
                message.append(this.id);
                message.append("' (Label: '");
                message.append(this.label);
                message.append("')!");
                throw new IllegalStateException(message.toString());
            }
            changed = true;
        }
        if (changed) {
            this.updateLifecycleStatus(ElementLifecycleEvent.ADJACENT_EDGE_ADDED_OR_REMOVED);
        }
    }

    @Override
    protected void reloadFromDatabase() {
        String id = this.id();
        ChronoGraphTransaction tx = this.getOwningTransaction();
        IVertexRecord vRecord = tx.getBackingDBTransaction().get(ChronoGraphConstants.KEYSPACE_VERTEX, id);
        this.withoutModificationCheck(() -> {
            this.clearPropertyStatusCache();
            this.labelToIncomingEdges = null;
            this.labelToOutgoingEdges = null;
            this.properties = null;
            if (vRecord != null) {
                this.recordReference = new WeakReference<>(vRecord);
                this.updateLifecycleStatus(ElementLifecycleEvent.RELOADED_FROM_DB_AND_IN_SYNC);
            } else {
                this.recordReference = null;
                this.updateLifecycleStatus(ElementLifecycleEvent.RELOADED_FROM_DB_AND_NO_LONGER_EXISTENT);
            }
        });
        this.updateLifecycleStatus(ElementLifecycleEvent.SAVED);
        this.getTransactionContext().registerLoadedVertex(this);
    }

    @SuppressWarnings({"unchecked"})
    private <V> VertexProperty<V> getSingleProperty(final String propertyKey) {
        PredefinedVertexProperty<V> predefinedProperty = ChronoGraphElementUtil.asPredefinedVertexProperty(this,
            propertyKey);
        if (predefinedProperty != null) {
            return predefinedProperty;
        }
        if (this.recordReference == null) {
            // we're not lazy -> use the regular property access
            return (VertexProperty<V>) this.properties.get(propertyKey);
        } else {
            // we're lazy -> use the record if necessary
            ChronoVertexProperty<?> chronoVertexProperty = null;
            if (this.properties != null) {
                chronoVertexProperty = this.properties.get(propertyKey);
            }
            if (chronoVertexProperty == null) {
                // fall back to creating it from the record
                IVertexRecord vertexRecord = this.getRecord();
                IVertexPropertyRecord record = vertexRecord.getProperty(propertyKey);
                if (record == null) {
                    // not found
                    return null;
                }
                // load this record
                chronoVertexProperty = this.loadPropertyRecord(record);
                // cache it
                if (this.properties == null) {
                    this.properties = Maps.newHashMap();
                }
                this.properties.put(propertyKey, chronoVertexProperty);
            }
            return (ChronoVertexProperty<V>) chronoVertexProperty;
        }
    }

    protected IVertexRecord getRecord() {
        if (this.recordReference == null) {
            return null;
        }
        IVertexRecord record = this.recordReference.get();
        if (record == null) {
            // reload
            ChronoGraphTransactionStatistics.getInstance().incrementNumberOfVertexRecordRefetches();
            record = this.owningTransaction.loadVertexRecord(this.id);
            this.recordReference = new WeakReference<>(record);
        }
        return record;
    }

    @Override
    public boolean isLazy() {
        if (this.recordReference == null) {
            // the record is NULL, therefore it has been loaded.
            return false;
        } else {
            // the record is non-NULL, therefore it has yet to
            // be loaded and this vertex is lazy.
            return true;
        }
    }

    // =====================================================================================================================
    // DEBUG OUTPUT
    // =====================================================================================================================


    private void logPropertyChange(final String key, final Object value) {
        if (!log.isTraceEnabled(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS)) {
            return;
        }
        // prepare some debug output
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append(ChronoGraphLoggingUtil.createLogHeader(this.owningTransaction));
        messageBuilder.append("Setting Property '");
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
        log.trace(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS, messageBuilder.toString());
    }

    private void logVertexRemove() {
        if (!log.isTraceEnabled(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS)) {
            return;
        }
        // prepare some debug output
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append(ChronoGraphLoggingUtil.createLogHeader(this.owningTransaction));
        messageBuilder.append("Removing Vertex ");
        messageBuilder.append(this.toString());
        messageBuilder.append(" (Object ID: ");
        messageBuilder.append(System.identityHashCode(this));
        messageBuilder.append(")");
        log.trace(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS, messageBuilder.toString());
    }

    private void logPropertyRemove(final String key) {
        if (!log.isTraceEnabled(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS)) {
            return;
        }
        // prepare some debug output
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append(ChronoGraphLoggingUtil.createLogHeader(this.owningTransaction));
        messageBuilder.append("Removing Property '");
        messageBuilder.append(key);
        messageBuilder.append("' from Vertex ");
        messageBuilder.append(this.toString());
        messageBuilder.append(" (Object ID: ");
        messageBuilder.append(System.identityHashCode(this));
        messageBuilder.append(")");
        log.trace(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS, messageBuilder.toString());
    }

    private void logAddEdge(final Vertex inVertex, final String edgeId, final boolean userProvidedId,
                            final String label) {
        if (!log.isTraceEnabled(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS)) {
            return;
        }
        // prepare some debug output
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append(ChronoGraphLoggingUtil.createLogHeader(this.owningTransaction));
        messageBuilder.append("Adding Edge. From Vertex: '");
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
        log.trace(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS, messageBuilder.toString());
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    protected void ensureVertexRecordIsLoaded() {
        if (this.recordReference == null) {
            // the record is NULL, therefore it has been loaded.
            return;
        }
        // the record isn't NULL, we need to load it
        this.loadRecordContents();
        this.recordReference = null;
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
