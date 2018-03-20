package org.chronos.chronodb.internal.impl.index;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.exceptions.ChronoDBQuerySyntaxException;
import org.chronos.chronodb.api.exceptions.InvalidIndexAccessException;
import org.chronos.chronodb.api.exceptions.UnknownIndexException;
import org.chronos.chronodb.api.indexing.DoubleIndexer;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.api.indexing.LongIndexer;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.internal.api.query.searchspec.DoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.LongSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification;
import org.chronos.chronodb.internal.impl.index.querycache.ChronoIndexQueryCache;
import org.chronos.chronodb.internal.impl.index.querycache.LRUIndexQueryCache;
import org.chronos.chronodb.internal.impl.index.querycache.NoIndexQueryCache;
import org.chronos.chronodb.internal.impl.index.setview.SetView;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryOperatorElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryQueryOperator;
import org.chronos.chronodb.internal.impl.query.parser.ast.QueryElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.WhereElement;
import org.chronos.common.autolock.AutoLock;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public abstract class AbstractIndexManager<C extends ChronoDBInternal> implements IndexManager {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final ChronoIndexQueryCache queryCache;
    private C owningDB;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    protected AbstractIndexManager(final C owningDB) {
        checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
        this.owningDB = owningDB;
        // check configuration to see if we want to have query caching
        ChronoDBConfiguration chronoDbConfig = this.getOwningDB().getConfiguration();
        if (chronoDbConfig.isIndexQueryCachingEnabled()) {
            int maxIndexQueryCacheSize = chronoDbConfig.getIndexQueryCacheMaxSize();
            boolean debugModeEnabled = chronoDbConfig.isDebugModeEnabled();
            this.queryCache = new LRUIndexQueryCache(maxIndexQueryCacheSize, debugModeEnabled);
        } else {
            // according to the configuration, no caching is required. To make sure that we still have
            // the same object structure (i.e. we don't have to deal with the cache object being NULL),
            // we create a pseudo-cache instead that actually "caches" nothing.
            this.queryCache = new NoIndexQueryCache();
        }
    }

    // =================================================================================================================
    // GETTERS
    // =================================================================================================================

    public C getOwningDB() {
        return this.owningDB;
    }

    // =================================================================================================================
    // INDEX QUERY METHODS
    // =================================================================================================================

    @Override
    public Set<String> queryIndex(final long timestamp, final Branch branch, final String keyspace,
                                  final SearchSpecification<?> searchSpec) {
        String property = searchSpec.getProperty();
        if (this.getIndexNames().contains(property) == false) {
            throw new UnknownIndexException("There is no index named '" + property + "'!");
        }
        this.assertIndexAccessIsOk(searchSpec);
        if (this.queryCache == null) {
            // cache disabled, return the result of the request directly
            return this.performIndexQuery(timestamp, branch, keyspace, searchSpec);
        } else {
            // cache enabled, pipe the request through the cache
            return this.queryCache.getOrCalculate(timestamp, branch, keyspace, searchSpec, () -> {
                return this.performIndexQuery(timestamp, branch, keyspace, searchSpec);
            });
        }
    }

    @Override
    public Iterator<QualifiedKey> evaluate(final long timestamp, final Branch branch, final ChronoDBQuery query) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must be >= 0!");
        checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
        try (AutoLock lock = this.getOwningDB().lockNonExclusive()) {
            // walk the AST of the query in a bottom-up fashion, applying the following strategy:
            // - WHERE node: run the query and remember the result set
            // - AND node: perform set intersection of left and right child result sets
            // - OR node: perform set union of left and right child result sets
            String keyspace = query.getKeyspace();
            QueryElement rootElement = query.getRootElement();
            Iterator<String> iterator = this.evaluateRecursive(rootElement, timestamp, branch, keyspace).iterator();
            return Iterators.transform(iterator, key -> QualifiedKey.create(keyspace, key));
        }
    }

    @Override
    public long evaluateCount(final long timestamp, final Branch branch, final ChronoDBQuery query) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must be >= 0!");
        checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
        try (AutoLock lock = this.getOwningDB().lockNonExclusive()) {
            // TODO PERFORMANCE: evaluating everything and then counting is not very efficient...
            String keyspace = query.getKeyspace();
            QueryElement rootElement = query.getRootElement();
            Set<String> resultSet = this.evaluateRecursive(rootElement, timestamp, branch, keyspace);
            return resultSet.size();
        }
    }

    // =================================================================================================================
    // ROLLBACK METHODS
    // =================================================================================================================

    @Override
    public void clearQueryCache() {
        if (this.queryCache != null) {
            this.queryCache.clear();
        }
    }

    @VisibleForTesting
    public ChronoIndexQueryCache getIndexQueryCache() {
        return this.queryCache;
    }

    // =================================================================================================================
    // ABSTRACT METHOD DECLARATIONS
    // =================================================================================================================

    protected abstract Set<String> performIndexQuery(final long timestamp, final Branch branch, String keyspace, final SearchSpecification<?> searchSpec);

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    protected Set<String> evaluateRecursive(final QueryElement element, final long timestamp, final Branch branch,
                                                  final String keyspace) {
        if (element instanceof BinaryOperatorElement) {
            BinaryOperatorElement binaryOpElement = (BinaryOperatorElement) element;
            // disassemble the element
            QueryElement left = binaryOpElement.getLeftChild();
            QueryElement right = binaryOpElement.getRightChild();
            BinaryQueryOperator op = binaryOpElement.getOperator();
            // recursively evaluate left and right child result sets
            Set<String> leftResult = this.evaluateRecursive(left, timestamp, branch, keyspace);
            Set<String> rightResult = this.evaluateRecursive(right, timestamp, branch, keyspace);
            Set<String> resultSet;
            // depending on the operator, perform union or intersection
            switch (op) {
                case AND:
                    resultSet = SetView.intersection(leftResult, rightResult);
                    break;
                case OR:
                    resultSet = SetView.union(leftResult, rightResult);
                    break;
                default:
                    throw new UnknownEnumLiteralException(
                        "Encountered unknown literal of BinaryQueryOperator: '" + op + "'!");
            }
            //  note: set views are always unmodifiable
            return resultSet;
        } else if (element instanceof WhereElement) {
            WhereElement<?, ?> whereElement = (WhereElement<?, ?>) element;
            // disassemble and execute the atomic query
            SearchSpecification<?> searchSpec = whereElement.toSearchSpecification();
            Set<String> keys = this.queryIndex(timestamp, branch, keyspace, searchSpec);
            return Collections.unmodifiableSet(keys);
        } else {
            // all other elements should be eliminated by optimizations...
            throw new ChronoDBQuerySyntaxException("Query contains unsupported element of class '"
                + element.getClass().getName() + "' - was the query optimized?");
        }
    }

    protected void assertIndexAccessIsOk(final SearchSpecification<?> searchSpec) {
        String indexName = searchSpec.getProperty();
        Set<Indexer<?>> indexers = this.getIndexersByIndexName().get(indexName);
        if (indexers == null || indexers.isEmpty()) {
            throw new UnknownIndexException("There is no index named '" + indexName + "'!");
        }
        boolean isStringIndex = indexers.stream().allMatch(indexer -> indexer instanceof StringIndexer);
        boolean isLongIndex = indexers.stream().allMatch(indexer -> indexer instanceof LongIndexer);
        boolean isDoubleIndex = indexers.stream().allMatch(indexer -> indexer instanceof DoubleIndexer);
        if (!isStringIndex && !isLongIndex && !isDoubleIndex) {
            throw new IllegalStateException("Could not determine index type of index '" + indexName + "'!");
        }
        if (isStringIndex && searchSpec instanceof StringSearchSpecification == false) {
            throw new InvalidIndexAccessException("Cannot access String index '" + indexName + "' with " + searchSpec.getDescriptiveSearchType() + " search [" + searchSpec + "]!");
        }
        if (isLongIndex && searchSpec instanceof LongSearchSpecification == false) {
            throw new InvalidIndexAccessException("Cannot access Long index '" + indexName + "' with " + searchSpec.getDescriptiveSearchType() + " search [" + searchSpec + "]!");
        }
        if (isDoubleIndex && searchSpec instanceof DoubleSearchSpecification == false) {
            throw new InvalidIndexAccessException("Cannot access Double index '" + indexName + "' with " + searchSpec.getDescriptiveSearchType() + " search [" + searchSpec + "]!");
        }
    }
}
