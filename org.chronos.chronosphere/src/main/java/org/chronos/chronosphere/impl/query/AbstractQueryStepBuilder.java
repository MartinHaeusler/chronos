package org.chronos.chronosphere.impl.query;

import static com.google.common.base.Preconditions.*;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.api.exceptions.ChronoSphereQueryException;
import org.chronos.chronosphere.api.query.QueryStepBuilder;
import org.chronos.chronosphere.api.query.QueryStepBuilderInternal;
import org.chronos.chronosphere.api.query.UntypedQueryStepBuilder;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public abstract class AbstractQueryStepBuilder<S, E> implements QueryStepBuilderInternal<S, E> {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	protected final QueryStepBuilderInternal<S, ?> previous;
	private final GraphTraversal<?, E> traversal;

	private ChronoSphereTransactionInternal transaction;
	private boolean evaluationStarted = false;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected AbstractQueryStepBuilder(final QueryStepBuilderInternal<S, ?> previous,
			final GraphTraversal<?, E> traversal) {
		checkNotNull(traversal, "Precondition violation - argument 'traversal' must not be NULL!");
		this.previous = previous;
		this.traversal = traversal;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	@SuppressWarnings("unchecked")
	public QueryStepBuilder<S, E> filter(final Predicate<E> predicate) {
		checkNotNull(predicate, "Precondition violation - argument 'predicate' must not be NULL!");
		this.assertModificationsAllowed();
		return new ObjectQueryStepBuilderImpl<S, E>(this, this.getTraversal().filter(traverser -> {
			Object object = traverser.get();
			return predicate.test((E) object);
		}));
	}

	@Override
	public QueryStepBuilder<S, E> limit(final long limit) {
		checkArgument(limit >= 0, "Precondition violation - argument 'limit' must not be negative!");
		this.assertModificationsAllowed();
		return new ObjectQueryStepBuilderImpl<S, E>(this, this.getTraversal().limit(limit));
	}

	@Override
	public QueryStepBuilder<S, E> orderBy(final Comparator<E> comparator) {
		checkNotNull(comparator, "Precondition violation - argument 'comparator' must not be NULL!");
		this.assertModificationsAllowed();
		return new ObjectQueryStepBuilderImpl<S, E>(this, this.getTraversal().order().by(comparator));
	}

	@Override
	public QueryStepBuilder<S, E> distinct() {
		this.assertModificationsAllowed();
		return new ObjectQueryStepBuilderImpl<S, E>(this, this.getTraversal().dedup());
	}

	@Override
	public <T> UntypedQueryStepBuilder<S, T> map(final Function<E, T> function) {
		checkNotNull(function, "Precondition violation - argument 'function' must not be NULL!");
		this.assertModificationsAllowed();
		return new ObjectQueryStepBuilderImpl<S, T>(this, this.getTraversal().map(t -> {
			return function.apply(t.get());
		}));
	}

	@Override
	public <T> UntypedQueryStepBuilder<S, T> flatMap(final Function<E, Iterator<T>> function) {
		checkNotNull(function, "Precondition violation - argument 'function' must not be NULL!");
		this.assertModificationsAllowed();
		return new ObjectQueryStepBuilderImpl<S, T>(this, this.getTraversal().flatMap(t -> {
			return function.apply(t.get());
		}));
	}

	@Override
	public QueryStepBuilder<S, E> notNull() {
		this.assertModificationsAllowed();
		return this.filter(e -> e != null);
	}

	@Override
	public QueryStepBuilder<S, E> named(final String stepName) {
		checkNotNull(stepName, "Precondition violation - argument 'stepName' must not be NULL!");
		this.assertModificationsAllowed();
		return new ObjectQueryStepBuilderImpl<S, E>(this, this.getTraversal().as(stepName));
	}

	@Override
	public UntypedQueryStepBuilder<S, Object> back(final String stepName) {
		checkNotNull(stepName, "Precondition violation - argument 'stepName' must not be NULL!");
		this.assertModificationsAllowed();
		return new ObjectQueryStepBuilderImpl<S, Object>(this, this.getTraversal().select(stepName));
	}

	@Override
	public QueryStepBuilder<S, E> except(final String stepName) {
		checkNotNull(stepName, "Precondition violation - argument 'stepName' must not be NULL!");
		this.assertModificationsAllowed();
		// syntax according to: https://groups.google.com/d/msg/gremlin-users/EZUU00UEdoY/nX11hMu4AgAJ
		return new ObjectQueryStepBuilderImpl<S, E>(this, this.getTraversal().where(P.neq(stepName)));
	}

	@Override
	public QueryStepBuilder<S, E> except(final Set<?> elementsToExclude) {
		checkNotNull(elementsToExclude, "Precondition violation - argument 'elementsToExclude' must not be NULL!");
		this.assertModificationsAllowed();
		return new ObjectQueryStepBuilderImpl<>(this, this.getTraversal().filter(t -> {
			return elementsToExclude.contains(t.get()) == false;
		}));
	}

	@Override
	@SuppressWarnings("unchecked")
	public UntypedQueryStepBuilder<S, Object> union(final QueryStepBuilder<E, ?>... subqueries) {
		checkNotNull(subqueries, "Precondition violation - argument 'subqueries' must not be NULL!");
		checkArgument(subqueries.length > 0,
				"Precondition violation - argument 'subqueries' must not be an empty array!");
		this.assertModificationsAllowed();
		this.setTransactionOnSubqueries(subqueries);
		GraphTraversal<?, ?>[] traversalsArray = this.extractTraversals(subqueries);
		GraphTraversal<?, Object> unionTraversal = this.getTraversal().union((Traversal<?, Object>[]) traversalsArray);
		return new ObjectQueryStepBuilderImpl<S, Object>(this, unionTraversal);
	}

	@Override
	@SuppressWarnings("unchecked")
	public QueryStepBuilder<S, E> and(final QueryStepBuilder<E, ?>... subqueries) {
		checkNotNull(subqueries, "Precondition violation - argument 'subqueries' must not be NULL!");
		checkArgument(subqueries.length > 0,
				"Precondition violation - argument 'subqueries' must not be an empty array!");
		this.assertModificationsAllowed();
		this.setTransactionOnSubqueries(subqueries);
		GraphTraversal<?, ?>[] traversalsArray = this.extractTraversals(subqueries);
		GraphTraversal<?, E> andTraversal = this.getTraversal().and(traversalsArray);
		return new ObjectQueryStepBuilderImpl<S, E>(this, andTraversal);
	}

	@Override
	@SuppressWarnings("unchecked")
	public QueryStepBuilder<S, E> or(final QueryStepBuilder<E, ?>... subqueries) {
		checkNotNull(subqueries, "Precondition violation - argument 'subqueries' must not be NULL!");
		checkArgument(subqueries.length > 0,
				"Precondition violation - argument 'subqueries' must not be an empty array!");
		this.assertModificationsAllowed();
		this.setTransactionOnSubqueries(subqueries);
		GraphTraversal<?, ?>[] traversalsArray = this.extractTraversals(subqueries);
		GraphTraversal<?, E> orTraversal = this.getTraversal().or(traversalsArray);
		return new ObjectQueryStepBuilderImpl<S, E>(this, orTraversal);
	}

	@Override
	public QueryStepBuilder<S, E> not(final QueryStepBuilder<E, ?> subquery) {
		checkNotNull(subquery, "Precondition violation - argument 'subquery' must not be NULL!");
		this.assertModificationsAllowed();
		this.setTransactionOnSubqueries(subquery);
		GraphTraversal<?, ?> traversal = ((QueryStepBuilderInternal<E, ?>) subquery).getTraversal();
		GraphTraversal<?, E> negatedTraversal = this.getTraversal().not(traversal);
		return new ObjectQueryStepBuilderImpl<S, E>(this, negatedTraversal);
	}

	@Override
	public Set<E> toSet() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		return this.getTraversal().toSet();
	}

	@Override
	public List<E> toList() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		return this.getTraversal().toList();
	}

	@Override
	public Stream<E> toStream() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		return this.getTraversal().toStream();
	}

	@Override
	public long count() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		// TODO this is not really optimal. Is there a smarter way in gremlin?
		return Iterators.size(this.traversal);
	}

	@Override
	public boolean hasNext() {
		this.preventAnyFurtherModifications();
		return this.getTraversal().hasNext();
	}

	@Override
	public E next() {
		this.preventAnyFurtherModifications();
		return this.getTraversal().next();
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	@Override
	public QueryStepBuilderInternal<S, ?> getPrevious() {
		return this.previous;
	}

	@Override
	public GraphTraversal<?, E> getTraversal() {
		return this.traversal;
	}

	@Override
	public void setTransaction(final ChronoSphereTransactionInternal transaction) {
		this.transaction = transaction;
	}

	@Override
	public ChronoSphereTransactionInternal getTransaction() {
		if (this.transaction == null && this.getPrevious() != null) {
			this.transaction = this.getPrevious().getTransaction();
		}
		return this.transaction;
	}

	// =================================================================================================================
	// HELPER METHODS
	// =================================================================================================================

	protected void assertModificationsAllowed() {
		if (this.evaluationStarted) {
			// evaluation of the query has already started - no more modifications are allowed!
			throw new ChronoSphereQueryException(
					"Evaluation on this query has already started ('next()' or 'hasNext()' was called)."
							+ " No further modifications are permitted!");
		}
	}

	protected void preventAnyFurtherModifications() {
		QueryStepBuilderInternal<S, ?> builder = this;
		while (builder != null) {
			if (builder instanceof AbstractQueryStepBuilder) {
				((AbstractQueryStepBuilder<S, ?>) builder).evaluationStarted = true;
			}
			builder = builder.getPrevious();
		}
	}

	protected <C> GraphTraversal<?, C> castTraversalTo(final Class<C> clazz) {
		return this.getTraversal().filter(t -> clazz.isInstance(t.get())).map(t -> clazz.cast(t.get()));
	}

	protected <C> GraphTraversal<?, C> castTraversalToNumeric(final Function<Number, C> conversion) {
		return this.getTraversal().filter(t -> t.get() instanceof Number).map(t -> conversion.apply((Number) t.get()));
	}

	protected GraphTraversal<?, ?>[] extractTraversals(final QueryStepBuilder<?, ?>... subqueries) {
		List<QueryStepBuilder<?, ?>> subQueryList = Lists.newArrayList(subqueries);
		List<GraphTraversal<?, ?>> traversals = subQueryList.stream()
				.map(query -> ((QueryStepBuilderInternal<?, ?>) query).getTraversal()).collect(Collectors.toList());
		GraphTraversal<?, ?>[] traversalsArray = traversals.toArray(new GraphTraversal<?, ?>[traversals.size()]);
		return traversalsArray;
	}

	protected void setTransactionOnSubqueries(final QueryStepBuilder<?, ?>... childSteps) {
		for (QueryStepBuilder<?, ?> builder : childSteps) {
			((QueryStepBuilderInternal<?, ?>) builder).setTransaction(this.getTransaction());
		}
	}

}
