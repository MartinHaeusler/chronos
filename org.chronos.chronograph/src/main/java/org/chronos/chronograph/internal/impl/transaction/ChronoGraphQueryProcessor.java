package org.chronos.chronograph.internal.impl.transaction;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.exceptions.ChronoDBQuerySyntaxException;
import org.chronos.chronodb.api.exceptions.UnknownKeyspaceException;
import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.internal.api.query.searchspec.DoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.LongSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryOperatorElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryQueryOperator;
import org.chronos.chronodb.internal.impl.query.parser.ast.QueryElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.WhereElement;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoProperty;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.util.ChronoGraphQueryUtil;
import org.chronos.chronograph.internal.impl.util.ChronoProxyUtil;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.util.ReflectionUtils;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class ChronoGraphQueryProcessor {

	private final StandardChronoGraphTransaction tx;

	public ChronoGraphQueryProcessor(final StandardChronoGraphTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		this.tx = tx;
	}

	public Iterator<Vertex> getAllVerticesIterator() {
		ChronoDBTransaction tx = this.tx.getBackingDBTransaction();
		Set<String> keySet = Sets.newHashSet();
		try {
			keySet = tx.keySet(ChronoGraphConstants.KEYSPACE_VERTEX);
		} catch (UnknownKeyspaceException ignored) {
		}
		GraphTransactionContext context = this.tx.getContext();
		if (context.isDirty() == false) {
			// no transient modifications; return the persistent state directly
			return new VertexResolvingIterator(keySet.iterator(), ElementLoadMode.LAZY);
		}
		// our context is dirty, therefore we have to add all new vertices and remove all deleted vertices
		Set<String> modifiedKeySet = Sets.newHashSet();
		// TODO PERFORMANCE ChronoGraph: refactor this once we have proper "is new" handling for transient vertices
		// check for all vertices if they were removed
		Set<String> removedVertexIds = Sets.newHashSet();
		for (ChronoVertexImpl vertex : context.getModifiedVertices()) {
			String id = vertex.id().toString();
			if (vertex.isRemoved()) {
				removedVertexIds.add(id);
			} else {
				modifiedKeySet.add(id);
			}
		}
		for (String id : keySet) {
			if (removedVertexIds.contains(id) == false) {
				modifiedKeySet.add(id);
			}
		}
		Iterator<Vertex> resultIterator = new VertexResolvingIterator(modifiedKeySet.iterator(), ElementLoadMode.LAZY);
		return ChronoProxyUtil.replaceVerticesByProxies(resultIterator, this.tx);
	}

	public Iterator<Vertex> getVerticesIterator(final Iterable<String> chronoVertexIds,
			final ElementLoadMode loadMode) {
		checkNotNull(chronoVertexIds, "Precondition violation - argument 'chronoVertexIds' must not be NULL!");
		checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
		GraphTransactionContext context = this.tx.getContext();
		Set<String> modifiedSet = Sets.newHashSet(chronoVertexIds);
		if (context.isDirty()) {
			// consider deleted vertices
			for (String vertexId : chronoVertexIds) {
				if (context.isVertexModified(vertexId) == false) {
					// vertex is not modified, keep it
					modifiedSet.add(vertexId);
				} else {
					// vertex may have been removed
					ChronoVertexImpl vertex = context.getModifiedVertex(vertexId);
					if (vertex.isRemoved() == false) {
						// vertex still exists, keep it.
						// We have to add it to the set because it may have been
						// added during this transaction. If it was just modified,
						// it will already be in the set and the 'add' operation
						// is a no-op.
						modifiedSet.add(vertexId);
					} else {
						// vertex was removed, drop it
						modifiedSet.remove(vertexId);
					}
				}
			}
		}
		Iterator<Vertex> resultIterator = new VertexResolvingIterator(modifiedSet.iterator(), loadMode);
		return ChronoProxyUtil.replaceVerticesByProxies(resultIterator, this.tx);
	}

	public Iterator<Vertex> getVerticesBySearchSpecifications(final Set<SearchSpecification<?>> searchSpecifications) {
		checkNotNull(searchSpecifications,
				"Precondition violation - argument 'searchSpecifications' must not be NULL!");
		SetMultimap<String, SearchSpecification<?>> propertyToSearchSpecifications = HashMultimap.create();
		for (SearchSpecification<?> spec : searchSpecifications) {
			propertyToSearchSpecifications.put(spec.getProperty(), spec);
		}
		ChronoGraphIndexManagerInternal indexManager = this.getIndexManager();
		Set<String> indexedProperties = indexManager.getIndexedVertexPropertyNames();
		Iterator<Vertex> resultIterator = null;
		if (indexedProperties.containsAll(propertyToSearchSpecifications.keySet())) {
			// pure index query
			resultIterator = this.performVertexQueryOnIndex(searchSpecifications);
		} else {
			// some properties are not indexed -> may require iteration
			Set<String> indexedPropertiesToUse = Sets.intersection(indexedProperties,
					propertyToSearchSpecifications.keySet());
			if (indexedPropertiesToUse.isEmpty()) {
				// none of the given properties is indexed; full graph iteration is required!
				ChronoLogger.logWarning(
						"Query requires iteration over all vertices, because none of the given properties is indexed!"
								+ " For better performance use indices. Requested properties: "
								+ propertyToSearchSpecifications.keySet().toString());
				Iterator<Vertex> allVerticesIterator = this.tx.getAllVerticesIterator();
				Predicate<Vertex> filterPredicate = new PropertyValueFilterPredicate<>(searchSpecifications);
				resultIterator = Iterators.filter(allVerticesIterator, filterPredicate);
			} else {
				// at least one of the given properties is indexed; do index query with post-processing filter
				Set<SearchSpecification<?>> indexedSearches = Sets.newHashSet();
				for (String indexedProperty : indexedPropertiesToUse) {
					Set<SearchSpecification<?>> set = propertyToSearchSpecifications.get(indexedProperty);
					indexedSearches.addAll(set);
				}
				Iterator<Vertex> indexIterator = this.performVertexQueryOnIndex(indexedSearches);
				// prepare the map of properties we need to filter manually
				Set<SearchSpecification<?>> nonIndexedSearches = Sets.newHashSet(searchSpecifications);
				nonIndexedSearches.removeAll(indexedSearches);
				Predicate<Vertex> filterPredicate = new PropertyValueFilterPredicate<>(nonIndexedSearches);
				resultIterator = Iterators.filter(indexIterator, filterPredicate);
			}
		}

		// replace vertices by proxies
		return ChronoProxyUtil.replaceVerticesByProxies(resultIterator, this.tx);
	}

	public Iterator<Vertex> getVerticesByProperties(final Map<String, Object> propertyKeyToPropertyValue) {
		checkNotNull(propertyKeyToPropertyValue,
				"Precondition violation - argument 'propertyKeyToPropertyValue' must not be NULL!");
		Set<SearchSpecification<?>> searchSpecifications = equalityMapToSearchSpecifications(propertyKeyToPropertyValue);
		return this.getVerticesBySearchSpecifications(searchSpecifications);
	}

	public Set<Vertex> evaluateVertexQuery(final ChronoDBQuery query) {
		checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
		QueryElement rootElement = query.getRootElement();
		Set<Vertex> vertices = this.evaluateQueryRecursive(rootElement, Vertex.class);
		return vertices;
	}

	public Iterator<Edge> getAllEdgesIterator() {
		ChronoDBTransaction tx = this.tx.getBackingDBTransaction();
		Set<String> keySet = Sets.newHashSet();
		try {
			keySet = tx.keySet(ChronoGraphConstants.KEYSPACE_EDGE);
		} catch (UnknownKeyspaceException ignored) {
		}
		GraphTransactionContext context = this.tx.getContext();
		if (context.isDirty() == false) {
			// no transient modifications; return the persistent state directly
			return new EdgeResolvingIterator(keySet.iterator());
		}
		// our context is dirty, therefore we have to add all new edges and remove all deleted edges
		Set<String> modifiedKeySet = Sets.newHashSet();
		// TODO PERFORMANCE ChronoGraph: refactor this once we have proper "is new" handling for transient edges
		// check for all edges if they were removed
		Set<String> removedEdgeIds = Sets.newHashSet();
		for (ChronoEdgeImpl edge : context.getModifiedEdges()) {
			String id = edge.id().toString();
			if (edge.isRemoved()) {
				removedEdgeIds.add(id);
			} else {
				modifiedKeySet.add(id);
			}
		}
		for (String id : keySet) {
			if (removedEdgeIds.contains(id) == false) {
				modifiedKeySet.add(id);
			}
		}
		Iterator<Edge> edges = new EdgeResolvingIterator(modifiedKeySet.iterator());
		return ChronoProxyUtil.replaceEdgesByProxies(edges, this.tx);
	}

	public Iterator<Edge> getEdgesIterator(final Iterable<String> chronoEdgeIds) {
		checkNotNull(chronoEdgeIds, "Precondition violation - argument 'chronoEdgeIds' must not be NULL!");
		GraphTransactionContext context = this.tx.getContext();
		Set<String> modifiedSet = Sets.newHashSet(chronoEdgeIds);
		if (context.isDirty()) {
			// consider deleted edges
			for (String edgeId : chronoEdgeIds) {
				if (context.isEdgeModified(edgeId) == false) {
					// edge is not modified, keep it
					modifiedSet.add(edgeId);
				} else {
					// edge may have been removed
					ChronoEdgeImpl edge = context.getModifiedEdge(edgeId);
					if (edge.isRemoved() == false) {
						// edge still exists, keep it.
						// We have to add it to the set because it may have been
						// added during this transaction. If it was just modified,
						// it will already be in the set and the 'add' operation
						// is a no-op.
						modifiedSet.add(edgeId);
					} else {
						// edge was removed, drop it
						modifiedSet.remove(edgeId);
					}
				}
			}
		}
		Iterator<Edge> edges = new EdgeResolvingIterator(modifiedSet.iterator());
		return ChronoProxyUtil.replaceEdgesByProxies(edges, this.tx);
	}

	public Iterator<Edge> getEdgesBySearchSpecifications(final Set<SearchSpecification<?>> searchSpecifications) {
		checkNotNull(searchSpecifications,
				"Precondition violation - argument 'searchSpecifications' must not be NULL!");
		SetMultimap<String, SearchSpecification<?>> propertyToSearchSpecifications = HashMultimap.create();
		for (SearchSpecification<?> spec : searchSpecifications) {
			propertyToSearchSpecifications.put(spec.getProperty(), spec);
		}
		ChronoGraphIndexManagerInternal indexManager = this.getIndexManager();
		Set<String> indexedProperties = indexManager.getIndexedEdgePropertyNames();
		Iterator<Edge> resultIterator = null;
		if (indexedProperties.containsAll(propertyToSearchSpecifications.keySet())) {
			// pure index query
			resultIterator = this.performEdgeQueryOnIndex(searchSpecifications);
		} else {
			// some properties are not indexed -> may require iteration
			Set<String> indexedPropertiesToUse = Sets.intersection(indexedProperties,
					propertyToSearchSpecifications.keySet());
			if (indexedPropertiesToUse.isEmpty()) {
				// none of the given properties is indexed; full graph iteration is required!
				ChronoLogger.logWarning(
						"Query requires iteration over all edges, because none of the given properties is indexed!"
								+ " For better performance use indices. Requested properties: "
								+ propertyToSearchSpecifications.keySet().toString());
				Iterator<Edge> allEdgesIterator = this.tx.getAllEdgesIterator();
				Predicate<Edge> filterPredicate = new PropertyValueFilterPredicate<>(searchSpecifications);
				resultIterator = Iterators.filter(allEdgesIterator, filterPredicate);
			} else {
				// at least one of the given properties is indexed; do index query with post-processing filter
				Set<SearchSpecification<?>> indexedSearches = Sets.newHashSet();
				for (String indexedProperty : indexedPropertiesToUse) {
					Set<SearchSpecification<?>> set = propertyToSearchSpecifications.get(indexedProperty);
					indexedSearches.addAll(set);
				}
				Iterator<Edge> indexIterator = this.performEdgeQueryOnIndex(indexedSearches);
				// prepare the map of properties we need to filter manually
				Set<SearchSpecification<?>> nonIndexedSearches = Sets.newHashSet(searchSpecifications);
				nonIndexedSearches.removeAll(indexedSearches);
				Predicate<Edge> filterPredicate = new PropertyValueFilterPredicate<>(nonIndexedSearches);
				resultIterator = Iterators.filter(indexIterator, filterPredicate);
			}
		}
		// replace vertices by proxies
		return ChronoProxyUtil.replaceEdgesByProxies(resultIterator, this.tx);
	}

	public Iterator<Edge> getEdgesByProperties(final Map<String, Object> propertyKeyToPropertyValue) {
		checkNotNull(propertyKeyToPropertyValue,
				"Precondition violation - argument 'propertyKeyToPropertyValue' must not be NULL!");
		Set<SearchSpecification<?>> searchSpecifications = equalityMapToSearchSpecifications(propertyKeyToPropertyValue);
		return this.getEdgesBySearchSpecifications(searchSpecifications);
	}

	public Set<Edge> evaluateEdgeQuery(final ChronoDBQuery query) {
		checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
		QueryElement rootElement = query.getRootElement();
		Set<Edge> edges = this.evaluateQueryRecursive(rootElement, Edge.class);
		return edges;
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private ChronoGraphIndexManagerInternal getIndexManager() {
		String branchName = this.tx.getBackingDBTransaction().getBranchName();
		return (ChronoGraphIndexManagerInternal) this.tx.getGraph().getIndexManager(branchName);
	}

	private Iterator<Vertex> performVertexQueryOnIndex(final Set<SearchSpecification<?>> searchSpecs) {
		checkNotNull(searchSpecs, "Precondition violation - argument 'searchSpecs' must not be NULL!");
		ChronoGraphIndexManagerInternal indexManager = this.getIndexManager();
		Iterator<String> indexQueryResultIdIterator = indexManager.findVertexIdsByIndexedProperties(searchSpecs);
		// we now enhance this iterator by looking at the modifications performed by the user in the transaction context
		if (this.tx.getContext().isDirty()) {
			// query context is dirty and requires post-processing to properly reflect the transient state
			List<ChronoProperty<?>> transientMatches = Lists.newArrayList();
			for (SearchSpecification<?> searchSpec : searchSpecs) {
				transientMatches.addAll(this.tx.getContext().getModifiedProperties(searchSpec));
			}
			Set<String> resultSet = Sets.newHashSet();
			PropertyValueFilterPredicate<Vertex> filterPredicate = new PropertyValueFilterPredicate<>(
					searchSpecs);
			// for every vertex in the index result set, check if it is modified or not
			while (indexQueryResultIdIterator.hasNext()) {
				String id = indexQueryResultIdIterator.next();
				if (this.tx.getContext().isVertexModified(id)) {
					// The vertex itself may have been modified, but the concrete property/properties we are
					// looking for may still be unmodified.
					Vertex modifiedVertex = this.tx.getContext().getModifiedVertex(id);
					if (filterPredicate.apply(this.tx.getContext().getOrCreateVertexProxy(modifiedVertex))) {
						// the filter matches the transient state of the vertex, so we keep it
						resultSet.add(id);
					} else {
						// the vertex was modified and the transient state does not match the filter,
						// so we discard that vertex by skipping it.
					}
				} else {
					// vertex is not modified; add it to the overall result
					resultSet.add(id);
				}
			}
			Set<Vertex> transientVertices = transientMatches.stream()
					// for each property, get the element to which it belongs
					.map(property -> property.element())
					// we are interested only in vertices (and cast to them)
					.filter(e -> e instanceof ChronoVertex).map(e -> (ChronoVertex) e)
					// filter out removed elements, they should never be part of a query result
					.filter(cVertex -> cVertex.isRemoved() == false)
					// collect the result to a set
					.collect(Collectors.toSet());
			// the set of search specs needs to be AND-connected, so we apply the filters one by one on the transient
			// state
			Set<Vertex> verticesToKeep = transientVertices;
			for (SearchSpecification<?> searchSpec : searchSpecs) {
				String property = searchSpec.getProperty();
				Set<Vertex> matchingVertices = Sets.newHashSet();
				for (Vertex vertex : verticesToKeep) {
					VertexProperty<?> matchProperty = vertex.property(property);
					Object propertyValue = null;
					if (matchProperty.isPresent()) {
						propertyValue = vertex.value(property);
					}
					if (ChronoGraphQueryUtil.searchSpecApplies(searchSpec, propertyValue)) {
						matchingVertices.add(vertex);
					}
				}
				verticesToKeep = matchingVertices;
			}
			Set<String> resultVertexIds = verticesToKeep.stream().map(v -> (String) v.id()).collect(Collectors.toSet());
			// everything that was modified transiently and matches the query has to be added to the result set
			resultSet.addAll(resultVertexIds);
			return new VertexResolvingIterator(resultSet.iterator(), ElementLoadMode.LAZY);
		} else {
			// query context is clean, no modifications, so index query delivers the result immediately
			return new VertexResolvingIterator(indexQueryResultIdIterator, ElementLoadMode.LAZY);
		}
	}

	private Iterator<Edge> performEdgeQueryOnIndex(final Set<SearchSpecification<?>> searchSpecs) {
		ChronoGraphIndexManagerInternal indexManager = this.getIndexManager();
		Iterator<String> indexQueryResultIdIterator = indexManager.findEdgeIdsByIndexedProperties(searchSpecs);
		// we now enhance this iterator by looking at the modifications performed by the user in the transaction context
		if (this.tx.getContext().isDirty()) {
			// query context is dirty and requires post-processing to properly reflect the transient state
			List<ChronoProperty<?>> transientMatches = Lists.newArrayList();
			for (SearchSpecification<?> searchSpec : searchSpecs) {
				transientMatches.addAll(this.tx.getContext().getModifiedProperties(searchSpec));
			}
			PropertyValueFilterPredicate<Edge> filterPredicate = new PropertyValueFilterPredicate<>(searchSpecs);
			Set<String> resultSet = Sets.newHashSet();
			// for every vertex in the index result set, check if it is modified or not
			while (indexQueryResultIdIterator.hasNext()) {
				String edgeId = indexQueryResultIdIterator.next();
				if (this.tx.getContext().isEdgeModified(edgeId)) {
					// The edge itself may have been modified, but the concrete property/properties we are
					// looking for may still be unmodified.
					Edge edge = this.tx.getContext().getModifiedEdge(edgeId);
					if (filterPredicate.apply(this.tx.getContext().getOrCreateEdgeProxy(edge))) {
						// the filter matches the transient state of the edge, so we keep it
						resultSet.add(edgeId);
					} else {
						// the edge was modified and the transient state does not match the filter,
						// so we discard that edge by skipping it.
					}
				} else {
					// edge is not modified; add it to the overall result
					resultSet.add(edgeId);
				}
			}
			Set<Edge> transientEdges = transientMatches.stream()
					// for each property, get the element to which it belongs
					.map(property -> property.element())
					// we are interested only in vertices (and cast to them)
					.filter(e -> e instanceof ChronoEdge).map(e -> (ChronoEdge) e)
					// filter out removed elements, they should never be part of a query result
					.filter(cEdge -> cEdge.isRemoved() == false)
					// collect the result to a set
					.collect(Collectors.toSet());
			// the set of search specs needs to be AND-connected, so we apply the filters one by one on the transient
			// state
			Set<Edge> edgesToKeep = transientEdges;
			for (SearchSpecification<?> searchSpec : searchSpecs) {
				String property = searchSpec.getProperty();
				Set<Edge> matchingEdges = Sets.newHashSet();
				for (Edge edge : edgesToKeep) {
					Property<?> matchProperty = edge.property(property);
					Object propertyValue = null;
					if (matchProperty.isPresent()) {
						propertyValue = edge.value(property);
					}
					if (ChronoGraphQueryUtil.searchSpecApplies(searchSpec, propertyValue)) {
						matchingEdges.add(edge);
					}
				}
				edgesToKeep = matchingEdges;
			}
			Set<String> resultEdgeIds = edgesToKeep.stream().map(v -> (String) v.id()).collect(Collectors.toSet());
			// everything that was modified transiently and matches the query has to be added to the result set
			resultSet.addAll(resultEdgeIds);
			return new EdgeResolvingIterator(resultSet.iterator());
		} else {
			// query context is clean, no modifications, so index query delivers the result immediately
			return new EdgeResolvingIterator(indexQueryResultIdIterator);
		}
	}

	@SuppressWarnings("unchecked")
	private <E extends Element> Set<E> evaluateQueryRecursive(final QueryElement element, final Class<E> clazz) {
		Set<E> resultSet = Sets.newHashSet();
		if (element instanceof BinaryOperatorElement) {
			BinaryOperatorElement binaryOpElement = (BinaryOperatorElement) element;
			// disassemble the element
			QueryElement left = binaryOpElement.getLeftChild();
			QueryElement right = binaryOpElement.getRightChild();
			BinaryQueryOperator op = binaryOpElement.getOperator();
			// recursively evaluate left and right child result sets
			Set<E> leftResult = this.evaluateQueryRecursive(left, clazz);
			Set<E> rightResult = this.evaluateQueryRecursive(right, clazz);
			// depending on the operator, perform union or intersection
			switch (op) {
			case AND:
				resultSet.addAll(leftResult);
				resultSet.retainAll(rightResult);
				break;
			case OR:
				resultSet.addAll(leftResult);
				resultSet.addAll(rightResult);
				break;
			default:
				throw new UnknownEnumLiteralException(
						"Encountered unknown literal of BinaryQueryOperator: '" + op + "'!");
			}
			return Collections.unmodifiableSet(resultSet);
		} else if (element instanceof WhereElement) {
			WhereElement<?, ?> whereElement = (WhereElement<?, ?>) element;
			// disassemble and execute the atomic query
			SearchSpecification<?> searchSpec = whereElement.toSearchSpecification();
			Iterator<E> iterator = null;
			if (Vertex.class.isAssignableFrom(clazz)) {
				// vertex query
				iterator = (Iterator<E>) this.getVerticesBySearchSpecifications(Collections.singleton(searchSpec));
			} else if (Edge.class.isAssignableFrom(clazz)) {
				// edge query
				iterator = (Iterator<E>) this.getEdgesBySearchSpecifications(Collections.singleton(searchSpec));
			} else {
				throw new IllegalArgumentException("Unknown subclass of Element: '" + clazz.getName() + "'!");
			}
			// convert to set
			while (iterator.hasNext()) {
				resultSet.add(iterator.next());
			}
			return Collections.unmodifiableSet(resultSet);
		} else {
			// all other elements should be eliminated by optimizations...
			throw new ChronoDBQuerySyntaxException("Query contains unsupported element of class '"
					+ element.getClass().getName() + "' - was the query optimized?");
		}
	}

	private static Set<SearchSpecification<?>> equalityMapToSearchSpecifications(final Map<String, Object> map) {
		Set<SearchSpecification<?>> searchSpecs = Sets.newHashSet();
		for (Entry<String, Object> entry : map.entrySet()) {
			String property = entry.getKey();
			Object searchValue = entry.getValue();
			if (searchValue == null) {
				throw new IllegalArgumentException("NULL cannot be used as a search value!");
			}
			if (searchValue instanceof String) {
				String stringVal = (String) searchValue;
				searchSpecs.add(StringSearchSpecification.create(property, Condition.EQUALS, TextMatchMode.STRICT, stringVal));
			} else if (ReflectionUtils.isLongCompatible(searchValue)) {
				long longVal = ReflectionUtils.asLong(searchValue);
				searchSpecs.add(LongSearchSpecification.create(property, Condition.EQUALS, longVal));
			} else if (ReflectionUtils.isDoubleCompatible(searchValue)) {
				double doubleVal = ReflectionUtils.asDouble(searchValue);
				searchSpecs.add(DoubleSearchSpecification.create(property, Condition.EQUALS, doubleVal, ChronoGraphQueryUtil.DOUBLE_EQUALITY_TOLERANCE));
			} else {
				throw new IllegalArgumentException("The value '" + searchValue + "' (class: " + searchValue.getClass().getName() + ") cannot be used for searching. Supported types are String, Long and Double.");
			}
		}
		return searchSpecs;
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	private class VertexResolvingIterator implements Iterator<Vertex> {

		private final ElementLoadMode loadMode;
		private final Iterator<?> idIterator;

		private Vertex nextVertex;

		private VertexResolvingIterator(final Iterator<?> idIterator, final ElementLoadMode loadMode) {
			checkNotNull(idIterator, "Precondition violation - argument 'idIterator' must not be NULL!");
			checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
			this.idIterator = idIterator;
			this.loadMode = loadMode;
			this.tryResolveNextVertex();
		}

		@Override
		public boolean hasNext() {
			return this.nextVertex != null;
		}

		@Override
		public Vertex next() {
			if (this.nextVertex == null) {
				throw new NoSuchElementException();
			}
			Vertex vertex = this.nextVertex;
			this.tryResolveNextVertex();
			return vertex;
		}

		private void tryResolveNextVertex() {
			while (this.idIterator.hasNext()) {
				// check if we have a vertex for this ID
				Object next = this.idIterator.next();
				String vertexId = null;
				if (next instanceof String) {
					vertexId = (String) next;
				} else {
					vertexId = String.valueOf(next);
				}
				Vertex vertex = ChronoGraphQueryProcessor.this.tx.loadVertex(vertexId, this.loadMode);
				if (vertex != null) {
					this.nextVertex = vertex;
					return;
				}
			}
			// we ran out of IDs -> there cannot be a next vertex
			this.nextVertex = null;
			return;
		}

	}

	private class EdgeResolvingIterator implements Iterator<Edge> {

		private final Iterator<?> idIterator;

		private Edge nextEdge;

		private EdgeResolvingIterator(final Iterator<?> idIterator) {
			checkNotNull(idIterator, "Precondition violation - argument 'idIterator' must not be NULL!");
			this.idIterator = idIterator;
			this.tryResolveNextEdge();
		}

		@Override
		public boolean hasNext() {
			return this.nextEdge != null;
		}

		@Override
		public Edge next() {
			if (this.nextEdge == null) {
				throw new NoSuchElementException();
			}
			Edge edge = this.nextEdge;
			this.tryResolveNextEdge();
			return edge;
		}

		private void tryResolveNextEdge() {
			while (this.idIterator.hasNext()) {
				// check if we have a vertex for this ID
				Object next = this.idIterator.next();
				String edgeId = null;
				if (next instanceof String) {
					edgeId = (String) next;
				} else {
					edgeId = String.valueOf(next);
				}
				Edge edge = ChronoGraphQueryProcessor.this.tx.loadEdge(edgeId);
				if (edge != null) {
					this.nextEdge = edge;
					return;
				}
			}
			// we ran out of IDs -> there cannot be a next edge
			this.nextEdge = null;
			return;
		}

	}

	private class PropertyValueFilterPredicate<V extends Element> implements Predicate<V> {

		private final Set<SearchSpecification<?>> searchSpecifications;

		private PropertyValueFilterPredicate(final Set<SearchSpecification<?>> searchSpecs) {
			checkNotNull(searchSpecs, "Precondition violation - argument 'searchSpecs' must not be NULL!");
			this.searchSpecifications = searchSpecs;
		}

		@Override
		public boolean apply(final V element) {
			ChronoElement chronoElement = (ChronoElement) element;
			if (chronoElement.isRemoved()) {
				// never consider removed elements
				return false;
			}
			for (SearchSpecification<?> searchSpec : this.searchSpecifications) {
				if (element.property(searchSpec.getProperty()).isPresent() == false) {
					// the property in question is not present, it is NOT possible to make
					// any decision if it matches the given search criterion or not. In particular,
					// when the search is negated (e.g. 'not equals'), we decide to have a non-match
					// for non-existing properties
					return false;
				}
				Object propertyValue = element.value(searchSpec.getProperty());
				boolean searchSpecApplies = ChronoGraphQueryUtil.searchSpecApplies(searchSpec, propertyValue);
				if (searchSpecApplies == false) {
					// element failed to pass this filter
					return false;
				}
			}
			// element passed all filters
			return true;
		}

	}
}
