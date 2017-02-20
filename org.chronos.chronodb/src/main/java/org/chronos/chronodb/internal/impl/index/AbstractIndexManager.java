package org.chronos.chronodb.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.exceptions.ChronoDBQuerySyntaxException;
import org.chronos.chronodb.api.exceptions.UnknownIndexException;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.Lockable.LockHolder;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.internal.api.query.SearchSpecification;
import org.chronos.chronodb.internal.impl.index.querycache.ChronoIndexQueryCache;
import org.chronos.chronodb.internal.impl.index.querycache.LRUIndexQueryCache;
import org.chronos.chronodb.internal.impl.index.querycache.NoIndexQueryCache;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryOperatorElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryQueryOperator;
import org.chronos.chronodb.internal.impl.query.parser.ast.QueryElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.WhereElement;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

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
	public Set<ChronoIdentifier> queryIndex(final long timestamp, final Branch branch,
			final SearchSpecification searchSpec) {
		String property = searchSpec.getProperty();
		if (this.getIndexNames().contains(property) == false) {
			throw new UnknownIndexException("There is no index named '" + property + "'!");
		}
		if (this.queryCache == null) {
			// cache disabled, return the result of the request directly
			return this.performIndexQuery(timestamp, branch, searchSpec);
		} else {
			// cache enabled, pipe the request through the cache
			return this.queryCache.getOrCalculate(timestamp, branch, searchSpec, () -> {
				return this.performIndexQuery(timestamp, branch, searchSpec);
			});
		}
	}

	@Override
	public Iterator<QualifiedKey> evaluate(final long timestamp, final Branch branch, final ChronoDBQuery query) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must be >= 0!");
		checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
		try (LockHolder lock = this.getOwningDB().lockNonExclusive()) {
			// walk the AST of the query in a bottom-up fashion, applying the following strategy:
			// - WHERE node: run the query and remember the result set
			// - AND node: perform set intersection of left and right child result sets
			// - OR node: perform set union of left and right child result sets
			String keyspace = query.getKeyspace();
			QueryElement rootElement = query.getRootElement();
			return this.evaluateRecursive(rootElement, timestamp, branch, keyspace).iterator();
		}
	}

	@Override
	public long evaluateCount(final long timestamp, final Branch branch, final ChronoDBQuery query) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must be >= 0!");
		checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
		try (LockHolder lock = this.getOwningDB().lockNonExclusive()) {
			// TODO PERFORMANCE: evaluating everything and then counting is not very efficient...
			String keyspace = query.getKeyspace();
			QueryElement rootElement = query.getRootElement();
			Set<QualifiedKey> resultSet = this.evaluateRecursive(rootElement, timestamp, branch, keyspace);
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

	protected abstract Set<ChronoIdentifier> performIndexQuery(final long timestamp, final Branch branch,
			final SearchSpecification searchSpec);

	// =================================================================================================================
	// HELPER METHODS
	// =================================================================================================================

	protected Set<QualifiedKey> evaluateRecursive(final QueryElement element, final long timestamp, final Branch branch,
			final String keyspace) {
		Set<QualifiedKey> resultSet = Sets.newHashSet();
		if (element instanceof BinaryOperatorElement) {
			BinaryOperatorElement binaryOpElement = (BinaryOperatorElement) element;
			// disassemble the element
			QueryElement left = binaryOpElement.getLeftChild();
			QueryElement right = binaryOpElement.getRightChild();
			BinaryQueryOperator op = binaryOpElement.getOperator();
			// recursively evaluate left and right child result sets
			Set<QualifiedKey> leftResult = this.evaluateRecursive(left, timestamp, branch, keyspace);
			Set<QualifiedKey> rightResult = this.evaluateRecursive(right, timestamp, branch, keyspace);
			// depending on the operator, perform union or intersection
			switch (op) {
			case AND:
				// FIXME: we must not discard entries from different timestamps!
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
			WhereElement whereElement = (WhereElement) element;
			// disassemble and execute the atomic query
			String indexName = whereElement.getIndexName();
			Condition condition = whereElement.getCondition();
			TextMatchMode matchMode = whereElement.getMatchMode();
			String comparisonValue = whereElement.getComparisonValue();
			SearchSpecification searchSpec = SearchSpecification.create(indexName, condition, matchMode,
					comparisonValue);
			Set<ChronoIdentifier> identifiers = this.queryIndex(timestamp, branch, searchSpec);
			// remove the non-matching keyspaces and reduce from ChronoIdentifier to qualified key
			Set<QualifiedKey> filtered = identifiers.parallelStream()
					// remove non-matching keyspaces
					.filter(id -> id.getKeyspace().equals(keyspace))
					// we don't need timestamps, so convert into qualified keys instead
					.map(id -> QualifiedKey.create(id.getKeyspace(), id.getKey()))
					// ... and collect everything in a set
					.collect(Collectors.toSet());
			return Collections.unmodifiableSet(filtered);
		} else {
			// all other elements should be eliminated by optimizations...
			throw new ChronoDBQuerySyntaxException("Query contains unsupported element of class '"
					+ element.getClass().getName() + "' - was the query optimized?");
		}
	}
}
