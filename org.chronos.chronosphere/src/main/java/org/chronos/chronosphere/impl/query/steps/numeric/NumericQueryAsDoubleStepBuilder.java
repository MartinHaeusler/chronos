package org.chronos.chronosphere.impl.query.steps.numeric;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.NumericQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.QueryUtils;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

public class NumericQueryAsDoubleStepBuilder<S> extends NumericQueryStepBuilderImpl<S, Object, Double> {

    public NumericQueryAsDoubleStepBuilder(final TraversalChainElement previous) {
        super(previous);
    }

    @Override
    public GraphTraversal<S, Double> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Object> traversal) {
        return QueryUtils.castTraversalToNumeric(traversal, Number::doubleValue);
    }
}
