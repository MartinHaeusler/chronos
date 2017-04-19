package org.chronos.chronosphere.api.query;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

public interface QueryStepBuilderInternal<S, E> extends QueryStepBuilder<S, E> {

	public void setTransaction(ChronoSphereTransactionInternal transaction);

	public ChronoSphereTransactionInternal getTransaction();

	public QueryStepBuilderInternal<S, ?> getPrevious();

	public GraphTraversal<?, E> getTraversal();

}
