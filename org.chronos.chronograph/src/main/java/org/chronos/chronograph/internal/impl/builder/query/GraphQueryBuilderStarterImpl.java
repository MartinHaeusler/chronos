package org.chronos.chronograph.internal.impl.builder.query;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.builder.query.FinalizableQueryBuilder;
import org.chronos.chronodb.api.builder.query.QueryBuilder;
import org.chronos.chronodb.api.builder.query.WhereBuilder;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronograph.api.builder.query.GraphFinalizableQueryBuilder;
import org.chronos.chronograph.api.builder.query.GraphQueryBuilder;
import org.chronos.chronograph.api.builder.query.GraphQueryBuilderStarter;
import org.chronos.chronograph.api.builder.query.GraphWhereBuilder;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.ChronoGraphConstants;

import com.google.common.collect.Iterators;

public class GraphQueryBuilderStarterImpl implements GraphQueryBuilderStarter {

	private final ChronoGraph graph;
	private final ChronoGraphTransaction tx;

	private QueryBuilder backendQuery;
	private WhereBuilder currentWhereQuery;
	private FinalizableQueryBuilder finalizableBackendQuery;
	private GraphQueryBuilderImpl<?> queryBuilder;
	private GraphWhereBuilderImpl<?> whereBuilder;
	private GraphFinalizableQueryBuilderImpl<?> finalizableBuilder;

	public GraphQueryBuilderStarterImpl(final ChronoGraph graph) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		this.graph = graph;
		this.graph.tx().readWrite();
		this.tx = graph.tx().getCurrentTransaction();
		this.queryBuilder = new GraphQueryBuilderImpl<>();
		this.whereBuilder = new GraphWhereBuilderImpl<>();
	}

	@Override
	@SuppressWarnings("unchecked")
	public GraphQueryBuilder<Vertex> vertices() {
		this.backendQuery = this.tx.getBackingDBTransaction().find().inKeyspace(ChronoGraphConstants.KEYSPACE_VERTEX);
		this.finalizableBuilder = new GraphFinalizableQueryBuilderImpl<>(Vertex.class);
		return (GraphQueryBuilder<Vertex>) this.queryBuilder;
	}

	@Override
	@SuppressWarnings("unchecked")
	public GraphQueryBuilder<Edge> edges() {
		this.backendQuery = this.tx.getBackingDBTransaction().find().inKeyspace(ChronoGraphConstants.KEYSPACE_EDGE);
		this.finalizableBuilder = new GraphFinalizableQueryBuilderImpl<>(Edge.class);
		return (GraphQueryBuilder<Edge>) this.queryBuilder;
	}

	private class GraphQueryBuilderImpl<E extends Element> implements GraphQueryBuilder<E> {

		@Override
		public GraphQueryBuilder<E> begin() {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			// forward to backing query
			self.backendQuery = self.backendQuery.begin();
			return this;
		}

		@Override
		public GraphQueryBuilder<E> end() {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			// forward to backing query
			self.backendQuery = self.backendQuery.end();
			return this;
		}

		@Override
		public GraphQueryBuilder<E> not() {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			// forward to backing query
			self.backendQuery = self.backendQuery.not();
			return this;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphWhereBuilder<E> where(final String property) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			self.currentWhereQuery = self.backendQuery.where(property);
			return (GraphWhereBuilder<E>) self.whereBuilder;
		}

	}

	private class GraphWhereBuilderImpl<E extends Element> implements GraphWhereBuilder<E> {

		// =================================================================================================================
		// STRING OPERATIONS
		// =================================================================================================================

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> contains(final String text) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.contains(text);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> containsIgnoreCase(final String text) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.containsIgnoreCase(text);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> notContains(final String text) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.notContains(text);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> notContainsIgnoreCase(final String text) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.notContainsIgnoreCase(text);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> startsWith(final String text) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.startsWith(text);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> startsWithIgnoreCase(final String text) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.startsWithIgnoreCase(text);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> notStartsWith(final String text) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.notStartsWith(text);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> notStartsWithIgnoreCase(final String text) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.notStartsWithIgnoreCase(text);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> endsWith(final String text) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.endsWith(text);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> endsWithIgnoreCase(final String text) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.endsWithIgnoreCase(text);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> notEndsWith(final String text) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.notEndsWith(text);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> notEndsWithIgnoreCase(final String text) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.notEndsWithIgnoreCase(text);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> matchesRegex(final String regex) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.matchesRegex(regex);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> notMatchesRegex(final String regex) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.notMatchesRegex(regex);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isEqualTo(final String value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isEqualTo(value);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isEqualToIgnoreCase(final String value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isEqualToIgnoreCase(value);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isNotEqualTo(final String value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isNotEqualTo(value);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isNotEqualToIgnoreCase(final String value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isNotEqualToIgnoreCase(value);
			self.currentWhereQuery = null;
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		// =================================================================================================================
		// LONG OPERATIONS
		// =================================================================================================================

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isEqualTo(final long value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isEqualTo(value);
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isNotEqualTo(final long value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isNotEqualTo(value);
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isGreaterThan(final long value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isGreaterThan(value);
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isGreaterThanOrEqualTo(final long value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isGreaterThanOrEqualTo(value);
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isLessThan(final long value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isLessThan(value);
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isLessThanOrEqualTo(final long value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isLessThanOrEqualTo(value);
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		// =================================================================================================================
		// DOUBLE OPERATIONS
		// =================================================================================================================

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isEqualTo(final double value, final double equalityTolerance) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isEqualTo(value, equalityTolerance);
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isNotEqualTo(final double value, final double equalityTolerance) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isNotEqualTo(value, equalityTolerance);
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isGreaterThan(final double value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isGreaterThan(value);
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isGreaterThanOrEqualTo(final double value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isGreaterThanOrEqualTo(value);
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isLessThan(final double value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isLessThan(value);
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphFinalizableQueryBuilder<E> isLessThanOrEqualTo(final double value) {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			if (self.currentWhereQuery == null) {
				throw new IllegalStateException("Cannot apply filter - no current 'where' clause is given!");
			}
			self.finalizableBackendQuery = self.currentWhereQuery.isLessThanOrEqualTo(value);
			return (GraphFinalizableQueryBuilder<E>) self.finalizableBuilder;
		}

	}

	private class GraphFinalizableQueryBuilderImpl<E extends Element> implements GraphFinalizableQueryBuilder<E> {

		private final Class<E> clazz;

		private GraphFinalizableQueryBuilderImpl(final Class<E> clazz) {
			checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
			this.clazz = clazz;
		}

		@Override
		public GraphFinalizableQueryBuilder<E> begin() {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			self.finalizableBackendQuery = self.finalizableBackendQuery.begin();
			return this;
		}

		@Override
		public GraphFinalizableQueryBuilder<E> end() {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			self.finalizableBackendQuery = self.finalizableBackendQuery.end();
			return this;
		}

		@Override
		public GraphFinalizableQueryBuilder<E> not() {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			self.finalizableBackendQuery = self.finalizableBackendQuery.not();
			return this;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Iterator<E> toIterator() {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			ChronoDBQuery query = self.finalizableBackendQuery.toQuery();
			if (Vertex.class.isAssignableFrom(this.clazz)) {
				// vertex iterator
				return (Iterator<E>) self.tx.evaluateVertexQuery(query).iterator();
			} else if (Edge.class.isAssignableFrom(this.clazz)) {
				// edge iterator
				return (Iterator<E>) self.tx.evaluateEdgeQuery(query).iterator();
			} else {
				throw new IllegalArgumentException("Unknown element class: '" + this.clazz.getName() + "'!");
			}
		}

		@Override
		@SuppressWarnings({ "unchecked" })
		public GraphTraversal<E, E> toTraversal() {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			Set<E> elements = this.toSet();
			if (elements.isEmpty()) {
				return new DefaultGraphTraversal<>(self.graph);
			}
			Object[] elementArray = elements.toArray();
			if (Vertex.class.isAssignableFrom(this.clazz)) {
				// vertex iterator
				return (GraphTraversal<E, E>) self.graph.traversal().V(elementArray);
			} else if (Edge.class.isAssignableFrom(this.clazz)) {
				// edge iterator
				return (GraphTraversal<E, E>) self.graph.traversal().E(elementArray);
			} else {
				throw new IllegalArgumentException("Unknown element class: '" + this.clazz.getName() + "'!");
			}
		}

		@Override
		public long count() {
			Iterator<E> iterator = this.toIterator();
			return Iterators.size(iterator);
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphQueryBuilder<E> and() {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			self.backendQuery = self.finalizableBackendQuery.and();
			self.finalizableBackendQuery = null;
			return (GraphQueryBuilder<E>) self.queryBuilder;
		}

		@Override
		@SuppressWarnings("unchecked")
		public GraphQueryBuilder<E> or() {
			GraphQueryBuilderStarterImpl self = GraphQueryBuilderStarterImpl.this;
			self.backendQuery = self.finalizableBackendQuery.or();
			self.finalizableBackendQuery = null;
			return (GraphQueryBuilder<E>) self.queryBuilder;
		}

	}

}
