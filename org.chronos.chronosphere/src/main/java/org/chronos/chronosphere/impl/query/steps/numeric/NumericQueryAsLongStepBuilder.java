package org.chronos.chronosphere.impl.query.steps.numeric;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.NumericQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.QueryUtils;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

public class NumericQueryAsLongStepBuilder<S> extends NumericQueryStepBuilderImpl<S, Object, Long> {

    public NumericQueryAsLongStepBuilder(final TraversalChainElement previous) {
        super(previous);
    }

    @Override
    public GraphTraversal<S, Long> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Object> traversal) {
        return QueryUtils.castTraversalToNumeric(traversal, Number::longValue);
    }
}
