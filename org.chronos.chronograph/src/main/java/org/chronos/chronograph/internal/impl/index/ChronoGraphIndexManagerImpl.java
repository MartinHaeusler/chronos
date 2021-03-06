package org.chronos.chronograph.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.builder.query.FinalizableQueryBuilder;
import org.chronos.chronodb.api.builder.query.QueryBuilder;
import org.chronos.chronodb.api.builder.query.WhereBuilder;
import org.chronos.chronodb.api.exceptions.UnknownKeyspaceException;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.api.query.searchspec.DoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.LongSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronograph.api.builder.index.IndexBuilderStarter;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexInternal;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
import org.chronos.chronograph.internal.api.index.IChronoGraphEdgeIndex;
import org.chronos.chronograph.internal.api.index.IChronoGraphVertexIndex;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.impl.builder.index.ChronoGraphIndexBuilder;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class ChronoGraphIndexManagerImpl implements ChronoGraphIndexManager, ChronoGraphIndexManagerInternal {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private ChronoGraphInternal graph;
	private final String branchName;

	private final Map<String, IChronoGraphVertexIndex> vertexIndices;
	private final Map<String, IChronoGraphEdgeIndex> edgeIndices;

	private final ReadWriteLock lock;

	public ChronoGraphIndexManagerImpl(final ChronoGraphInternal graph, final String branchName) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		this.graph = graph;
		this.branchName = branchName;
		this.vertexIndices = Maps.newHashMap();
		this.edgeIndices = Maps.newHashMap();
		this.lock = new ReentrantReadWriteLock(true);
		this.loadIndexDataFromDB();
	}

	// =====================================================================================================================
	// INDEX CREATION
	// =====================================================================================================================

	@Override
	public IndexBuilderStarter create() {
		return new ChronoGraphIndexBuilder(this);
	}

	// =====================================================================================================================
	// INDEX METADATA QUERYING
	// =====================================================================================================================

	@Override
	public Set<ChronoGraphIndex> getIndexedPropertiesOf(final Class<? extends Element> clazz) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		return this.performNonExclusive(() -> {
			if (Vertex.class.isAssignableFrom(clazz)) {
				return Collections.unmodifiableSet(Sets.newHashSet(this.vertexIndices.values()));
			} else if (Edge.class.isAssignableFrom(clazz)) {
				return Collections.unmodifiableSet(Sets.newHashSet(this.edgeIndices.values()));
			} else {
				throw new IllegalArgumentException("Unknown graph element class: '" + clazz.getName() + "'!");
			}
		});
	}

	@Override
	public boolean isReindexingRequired() {
		return this.performNonExclusive(() -> {
			return this.getChronoDBIndexManager().isReindexingRequired();
		});
	}

	@Override
	public Set<ChronoGraphIndex> getDirtyIndices() {
		return this.performNonExclusive(() -> {
			IndexManager indexManager = this.getChronoDBIndexManager();
			Set<String> dirtyBackendIndexKeys = indexManager.getDirtyIndices();
			Set<ChronoGraphIndex> dirtyGraphIndices = Sets.newHashSet();
			for (String dirtyBackendIndexKey : dirtyBackendIndexKeys) {
				ChronoGraphIndex graphIndex = this.getIndexForBackendPropertyKey(dirtyBackendIndexKey);
				dirtyGraphIndices.add(graphIndex);
			}
			return Collections.unmodifiableSet(dirtyGraphIndices);
		});
	}

	// =====================================================================================================================
	// INDEX MANIPULATION
	// =====================================================================================================================

	@Override
	public void reindexAll() {
		this.performExclusive(() -> this.getChronoDBIndexManager().reindexAll());
	}

	@Override
	public void reindex(final ChronoGraphIndex index) {
		checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
		this.reindexAll();
	}

	@Override
	public void dropIndex(final ChronoGraphIndex index) {
		checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
		this.performExclusive(() -> {
			ChronoGraphIndexInternal indexInternal = (ChronoGraphIndexInternal) index;
			IndexManager indexManager = this.getChronoDBIndexManager();
			indexManager.removeIndex(indexInternal.getBackendIndexKey());
			// FIXME CONSISTENCY: What happens if an exception occurs at this line (or JVM shutdown, or...)?
			ChronoDB db = this.getDB();
			ChronoDBTransaction tx = db.tx(this.branchName);
			tx.remove(indexInternal.getBackendIndexKey());
			tx.commit();
			// FIXME CONSISTENCY: What happens if an exception occurs at this line (or JVM shutdown, or...)?
			if (indexInternal instanceof IChronoGraphVertexIndex) {
				this.vertexIndices.remove(indexInternal.getBackendIndexKey());
			} else if (indexInternal instanceof IChronoGraphEdgeIndex) {
				this.edgeIndices.remove(indexInternal.getBackendIndexKey());
			}
		});
	}

	@Override
	public void dropAllIndices() {
		this.performExclusive(() -> {
			IndexManager indexManager = this.getChronoDBIndexManager();
			indexManager.clearAllIndices();
			// FIXME CONSISTENCY: What happens if an exception occurs at this line (or JVM shutdown, or...)?
			ChronoDB db = this.getDB();
			ChronoDBTransaction tx = db.tx(this.branchName);
			for (ChronoGraphIndex index : this.getAllIndices()) {
				tx.remove(((ChronoGraphIndexInternal) index).getBackendIndexKey());
			}
			tx.commit();
			// FIXME CONSISTENCY: What happens if an exception occurs at this line (or JVM shutdown, or...)?
			this.vertexIndices.clear();
			this.edgeIndices.clear();
		});
	}

	// =====================================================================================================================
	// INTERNAL API :: MODIFICATION
	// =====================================================================================================================

	@Override
	public void addIndex(final ChronoGraphIndex index) {
		checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
		this.performExclusive(() -> {
			if (this.getAllIndices().contains(index)) {
				throw new IllegalArgumentException("The given index already exists: " + index.toString());
			}
			ChronoGraphIndexInternal indexInternal = (ChronoGraphIndexInternal) index;
			Indexer<?> indexer = indexInternal.createIndexer();
			IndexManager indexManager = this.getChronoDBIndexManager();
			indexManager.addIndexer(indexInternal.getBackendIndexKey(), indexer);
			// FIXME CONSISTENCY: What happens if an exception occurs at this line (or JVM shutdown, or...)?
			ChronoDB db = this.getDB();
			ChronoDBTransaction tx = db.tx(this.branchName);
			tx.put(ChronoGraphConstants.KEYSPACE_MANAGEMENT_INDICES, indexInternal.getBackendIndexKey(), indexInternal);
			tx.commit();
			// FIXME CONSISTENCY: What happens if an exception occurs at this line (or JVM shutdown, or...)?
			if (indexInternal instanceof IChronoGraphVertexIndex) {
				this.vertexIndices.put(indexInternal.getBackendIndexKey(), (IChronoGraphVertexIndex) indexInternal);
			} else if (indexInternal instanceof IChronoGraphEdgeIndex) {
				this.edgeIndices.put(indexInternal.getBackendIndexKey(), (IChronoGraphEdgeIndex) indexInternal);
			} else {
				throw new IllegalArgumentException(
						"Unknown index class: '" + indexInternal.getClass().getName() + "'!");
			}
		});
	}

	// =====================================================================================================================
	// INTERNAL API :: SEARCH
	// =====================================================================================================================

	@Override
	public Iterator<String> findVertexIdsByIndexedProperties(final Set<SearchSpecification<?>> searchSpecifications) {
		checkNotNull(searchSpecifications,
				"Precondition violation - argument 'searchSpecifications' must not be NULL!");
		return this.findElementsByIndexedProperties(Vertex.class, ChronoGraphConstants.KEYSPACE_VERTEX,
				searchSpecifications);
	}

	@Override
	public Iterator<String> findEdgeIdsByIndexedProperties(final Set<SearchSpecification<?>> searchSpecifications) {
		checkNotNull(searchSpecifications,
				"Precondition violation - argument 'searchSpecifications' must not be NULL!");
		return this.findElementsByIndexedProperties(Edge.class, ChronoGraphConstants.KEYSPACE_EDGE,
				searchSpecifications);
	}

	private Iterator<String> findElementsByIndexedProperties(final Class<? extends Element> clazz,
			final String keyspace, final Set<SearchSpecification<?>> searchSpecifications) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(searchSpecifications,
				"Precondition violation - argument 'searchSpecifications' must not be NULL!");
		// we need to make sure that all of the given properties are indeed indexed
		checkArgument(searchSpecifications.isEmpty() == false,
				"Precondition violation - need at least one search specification to search for!");
		Set<String> properties = searchSpecifications.stream().map(spec -> spec.getProperty())
				.collect(Collectors.toSet());
		this.assertAllPropertiesAreIndexed(clazz, properties);
		// build a map from 'backend property key' to 'search specifications'
		SetMultimap<String, SearchSpecification<?>> backendPropertyKeyToSearchSpecs = HashMultimap.create();
		Set<ChronoGraphIndex> graphIndices = this.getIndexedPropertiesOf(clazz);
		for (SearchSpecification<?> searchSpec : searchSpecifications) {
			String propertyName = searchSpec.getProperty();
			ChronoGraphIndex index = graphIndices.stream().filter(idx -> idx.getIndexedProperty().equals(propertyName))
					.findAny().get();
			String backendPropertyKey = ((ChronoGraphIndexInternal) index).getBackendIndexKey();
			backendPropertyKeyToSearchSpecs.put(backendPropertyKey, searchSpec);
		}
		// assert that we have a transaction to the backend
		this.graph.tx().readWrite();
		// get the transaction
		ChronoDBTransaction backendTransaction = this.graph.tx().getCurrentTransaction().getBackingDBTransaction();
		// build the composite query
		QueryBuilder builder = backendTransaction.find().inKeyspace(keyspace);
		FinalizableQueryBuilder finalizableBuilder = null;
		List<String> propertyList = Lists.newArrayList(backendPropertyKeyToSearchSpecs.keySet());
		for (int i = 0; i < propertyList.size(); i++) {
			String propertyName = propertyList.get(i);
			Set<SearchSpecification<?>> searchSpecs = backendPropertyKeyToSearchSpecs.get(propertyName);
			FinalizableQueryBuilder innerTempBuilder = null;
			for (SearchSpecification<?> searchSpec : searchSpecs) {
				WhereBuilder whereBuilder = null;
				if (innerTempBuilder == null) {
					whereBuilder = builder.where(propertyName);
				} else {
					whereBuilder = innerTempBuilder.and().where(propertyName);
				}
				innerTempBuilder = this.applyCondition(whereBuilder, searchSpec);
			}

			if (i + 1 < propertyList.size()) {
				// continuation
				builder = innerTempBuilder.and();
			} else {
				// done
				finalizableBuilder = innerTempBuilder;
			}
		}
		// run the query
		Iterator<QualifiedKey> keys = finalizableBuilder.getKeys();
		// this is the "raw" iterator over vertex IDs which we obtain from our index.
		Iterator<String> indexQueryResultIdIterator = Iterators.transform(keys, qualifiedKey -> qualifiedKey.getKey());
		return indexQueryResultIdIterator;
	}

	// =====================================================================================================================
	// INTERNAL API :: GRAPH SWITCHING
	// For the purpose of threaded transaction graphs, it is necessary to replace our working graph temporarily with
	// the threaded graph, to make sure that the correct transaction is used (e.g. for queries).
	// =====================================================================================================================

	@Override
	public void executeOnGraph(final ChronoGraphInternal graph, final Runnable job) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(job, "Precondition violation - argument 'job' must not be NULL!");
		ChronoGraphInternal previousGraph = this.graph;
		try {
			this.graph = graph;
			// execute the task
			job.run();
			return;
		} finally {
			// reset to the previous graph
			this.graph = previousGraph;
		}
	}

	@Override
	public <T> T executeOnGraph(final ChronoGraphInternal graph, final Callable<T> job) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(job, "Precondition violation - argument 'job' must not be NULL!");
		ChronoGraphInternal previousGraph = this.graph;
		try {
			this.graph = graph;
			// execute the task
			return job.call();
		} catch (Exception e) {
			throw new RuntimeException("Execution of task failed", e);
		} finally {
			// reset to the previous graph
			this.graph = previousGraph;
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private ChronoDB getDB() {
		return this.graph.getBackingDB();
	}

	private IndexManager getChronoDBIndexManager() {
		return this.getDB().getIndexManager();
	}

	private void loadIndexDataFromDB() {
		ChronoDBTransaction tx = this.getDB().tx(this.branchName);
		Set<String> allIndexKeys = null;
		try {
			allIndexKeys = tx.keySet(ChronoGraphConstants.KEYSPACE_MANAGEMENT_INDICES);
		} catch (UnknownKeyspaceException e) {
			// we do not yet have any indices
			allIndexKeys = Collections.emptySet();
		}
		for (String indexKey : allIndexKeys) {
			ChronoGraphIndex index = tx.get(ChronoGraphConstants.KEYSPACE_MANAGEMENT_INDICES, indexKey);
			if (index == null) {
				throw new IllegalStateException("Could not find index specification for index key '" + indexKey + "'!");
			}
			index = IndexMigrationUtil.migrate(index);
			if (index instanceof IChronoGraphVertexIndex) {
				IChronoGraphVertexIndex vertexIndex = (IChronoGraphVertexIndex) index;
				this.vertexIndices.put(vertexIndex.getBackendIndexKey(), vertexIndex);
			} else if (index instanceof IChronoGraphEdgeIndex) {
				IChronoGraphEdgeIndex edgeIndex = (IChronoGraphEdgeIndex) index;
				this.edgeIndices.put(edgeIndex.getBackendIndexKey(), edgeIndex);
			} else {
				throw new IllegalStateException(
						"Loaded unknown graph index class: '" + index.getClass().getName() + "'!");
			}
		}
	}

	private ChronoGraphIndex getIndexForBackendPropertyKey(final String propertyKey) {
		checkNotNull(propertyKey, "Precondition violation - argument 'propertyKey' must not be NULL!");
		if (propertyKey.startsWith(ChronoGraphConstants.INDEX_PREFIX_VERTEX)) {
			return this.vertexIndices.get(propertyKey);
		} else if (propertyKey.startsWith(ChronoGraphConstants.INDEX_PREFIX_EDGE)) {
			return this.edgeIndices.get(propertyKey);
		} else {
			throw new IllegalArgumentException("The string '" + propertyKey + "' is no valid index property key!");
		}
	}

	private void performExclusive(final Runnable r) {
		this.lock.writeLock().lock();
		try {
			r.run();
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	@SuppressWarnings("unused")
	private <T> T performExclusive(final Callable<T> c) {
		this.lock.writeLock().lock();
		try {
			return c.call();
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while performing exclusive task: " + e.toString(), e);
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	@SuppressWarnings("unused")
	private void performNonExclusive(final Runnable r) {
		this.lock.readLock().lock();
		try {
			r.run();
		} finally {
			this.lock.readLock().unlock();
		}
	}

	private <T> T performNonExclusive(final Callable<T> c) {
		this.lock.readLock().lock();
		try {
			return c.call();
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while performing exclusive task: " + e.toString(), e);
		} finally {
			this.lock.readLock().unlock();
		}
	}

	private void assertAllPropertiesAreIndexed(final Class<? extends Element> clazz, final Set<String> propertyNames) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		checkNotNull(propertyNames, "Precondition violation - argument 'propertyNames' must not be NULL!");
		if (Vertex.class.isAssignableFrom(clazz)) {
			Set<ChronoGraphIndex> indexedVertexProperties = this.getIndexedVertexProperties();
			Set<String> indexedVertexPropertyNames = indexedVertexProperties.stream()
					.map(ivp -> ivp.getIndexedProperty()).collect(Collectors.toSet());
			Set<String> unindexedProperties = Sets.newHashSet(propertyNames);
			unindexedProperties.removeAll(indexedVertexPropertyNames);
			if (unindexedProperties.isEmpty() == false) {
				throw new IllegalArgumentException(
						"Some of the given properties are not indexed on vertices: " + unindexedProperties);
			}
		} else if (Edge.class.isAssignableFrom(clazz)) {
			Set<ChronoGraphIndex> indexedEdgeProperties = this.getIndexedEdgeProperties();
			Set<String> indexedEdgePropertyNames = indexedEdgeProperties.stream().map(ivp -> ivp.getIndexedProperty())
					.collect(Collectors.toSet());
			Set<String> unindexedProperties = Sets.newHashSet(propertyNames);
			unindexedProperties.removeAll(indexedEdgePropertyNames);
			if (unindexedProperties.isEmpty() == false) {
				throw new IllegalArgumentException(
						"Some of the given properties are not indexed on edges: " + unindexedProperties);
			}
		} else {
			throw new IllegalArgumentException("Unknown graph element class: '" + clazz.getName() + "'!");
		}
	}

	private FinalizableQueryBuilder applyCondition(final WhereBuilder whereBuilder,
			final SearchSpecification<?> searchSpec) {
		if (searchSpec instanceof StringSearchSpecification) {
			return this.applyCondition(whereBuilder, (StringSearchSpecification) searchSpec);
		} else if (searchSpec instanceof LongSearchSpecification) {
			return this.applyCondition(whereBuilder, (LongSearchSpecification) searchSpec);
		} else if (searchSpec instanceof DoubleSearchSpecification) {
			return this.applyCondition(whereBuilder, (DoubleSearchSpecification) searchSpec);
		} else {
			throw new IllegalStateException(
					"Unknown SearchSpecification class: '" + searchSpec.getClass().getName() + "'!");
		}
	}

	private FinalizableQueryBuilder applyCondition(final WhereBuilder whereBuilder,
			final StringSearchSpecification searchSpec) {
		StringCondition condition = searchSpec.getCondition();
		TextMatchMode matchMode = searchSpec.getMatchMode();
		String searchText = searchSpec.getSearchValue();
		if (condition.equals(StringCondition.CONTAINS)) {
			switch (matchMode) {
			case CASE_INSENSITIVE:
				return whereBuilder.containsIgnoreCase(searchText);
			case STRICT:
				return whereBuilder.contains(searchText);
			default:
				throw new UnknownEnumLiteralException(matchMode);
			}
		} else if (condition.equals(StringCondition.ENDS_WITH)) {
			switch (matchMode) {
			case CASE_INSENSITIVE:
				return whereBuilder.endsWithIgnoreCase(searchText);
			case STRICT:
				return whereBuilder.endsWith(searchText);
			default:
				throw new UnknownEnumLiteralException(matchMode);
			}
		} else if (condition.equals(StringCondition.EQUALS)) {
			switch (matchMode) {
			case CASE_INSENSITIVE:
				return whereBuilder.isEqualToIgnoreCase(searchText);
			case STRICT:
				return whereBuilder.isEqualTo(searchText);
			default:
				throw new UnknownEnumLiteralException(matchMode);
			}
		} else if (condition.equals(StringCondition.MATCHES_REGEX)) {
			return whereBuilder.matchesRegex(searchText);
		} else if (condition.equals(StringCondition.NOT_CONTAINS)) {
			switch (matchMode) {
			case CASE_INSENSITIVE:
				return whereBuilder.notContainsIgnoreCase(searchText);
			case STRICT:
				return whereBuilder.notContains(searchText);
			default:
				throw new UnknownEnumLiteralException(matchMode);
			}
		} else if (condition.equals(StringCondition.NOT_ENDS_WITH)) {
			switch (matchMode) {
			case CASE_INSENSITIVE:
				return whereBuilder.notEndsWithIgnoreCase(searchText);
			case STRICT:
				return whereBuilder.notEndsWith(searchText);
			default:
				throw new UnknownEnumLiteralException(matchMode);
			}
		} else if (condition.equals(StringCondition.NOT_EQUALS)) {
			switch (matchMode) {
			case CASE_INSENSITIVE:
				return whereBuilder.isNotEqualToIgnoreCase(searchText);
			case STRICT:
				return whereBuilder.isNotEqualTo(searchText);
			default:
				throw new UnknownEnumLiteralException(matchMode);
			}
		} else if (condition.equals(StringCondition.NOT_MATCHES_REGEX)) {
			return whereBuilder.notMatchesRegex(searchText);
		} else if (condition.equals(StringCondition.NOT_STARTS_WITH)) {
			switch (matchMode) {
			case CASE_INSENSITIVE:
				return whereBuilder.notStartsWithIgnoreCase(searchText);
			case STRICT:
				return whereBuilder.notStartsWith(searchText);
			default:
				throw new UnknownEnumLiteralException(matchMode);
			}
		} else if (condition.equals(StringCondition.STARTS_WITH)) {
			switch (matchMode) {
			case CASE_INSENSITIVE:
				return whereBuilder.startsWithIgnoreCase(searchText);
			case STRICT:
				return whereBuilder.startsWith(searchText);
			default:
				throw new UnknownEnumLiteralException(matchMode);
			}
		} else {
			throw new IllegalStateException("Unknown StringCondition: '" + condition.getClass().getName() + "'!");
		}
	}

	private FinalizableQueryBuilder applyCondition(final WhereBuilder whereBuilder,
			final LongSearchSpecification searchSpec) {
		NumberCondition condition = searchSpec.getCondition();
		long comparisonValue = searchSpec.getSearchValue();
		if (condition.equals(NumberCondition.EQUALS)) {
			return whereBuilder.isEqualTo(comparisonValue);
		} else if (condition.equals(NumberCondition.NOT_EQUALS)) {
			return whereBuilder.isNotEqualTo(comparisonValue);
		} else if (condition.equals(NumberCondition.LESS_EQUAL)) {
			return whereBuilder.isLessThanOrEqualTo(comparisonValue);
		} else if (condition.equals(NumberCondition.LESS_THAN)) {
			return whereBuilder.isLessThan(comparisonValue);
		} else if (condition.equals(NumberCondition.GREATER_EQUAL)) {
			return whereBuilder.isGreaterThanOrEqualTo(comparisonValue);
		} else if (condition.equals(NumberCondition.GREATER_THAN)) {
			return whereBuilder.isGreaterThan(comparisonValue);
		} else {
			throw new IllegalStateException("Unknown NumberCondition: '" + condition.getClass().getName() + "'!");
		}
	}

	private FinalizableQueryBuilder applyCondition(final WhereBuilder whereBuilder,
			final DoubleSearchSpecification searchSpec) {
		NumberCondition condition = searchSpec.getCondition();
		double comparisonValue = searchSpec.getSearchValue();
		double equalityTolerance = searchSpec.getEqualityTolerance();
		if (condition.equals(NumberCondition.EQUALS)) {
			return whereBuilder.isEqualTo(comparisonValue, equalityTolerance);
		} else if (condition.equals(NumberCondition.NOT_EQUALS)) {
			return whereBuilder.isNotEqualTo(comparisonValue, equalityTolerance);
		} else if (condition.equals(NumberCondition.LESS_EQUAL)) {
			return whereBuilder.isLessThanOrEqualTo(comparisonValue);
		} else if (condition.equals(NumberCondition.LESS_THAN)) {
			return whereBuilder.isLessThan(comparisonValue);
		} else if (condition.equals(NumberCondition.GREATER_EQUAL)) {
			return whereBuilder.isGreaterThanOrEqualTo(comparisonValue);
		} else if (condition.equals(NumberCondition.GREATER_THAN)) {
			return whereBuilder.isGreaterThan(comparisonValue);
		} else {
			throw new IllegalStateException("Unknown NumberCondition: '" + condition.getClass().getName() + "'!");
		}
	}
}
