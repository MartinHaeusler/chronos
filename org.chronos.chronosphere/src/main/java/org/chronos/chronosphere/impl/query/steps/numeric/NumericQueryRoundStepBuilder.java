package org.chronos.chronosphere.impl.query.steps.numeric;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.NumericQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

public class NumericQueryRoundStepBuilder<S, N extends Number> extends NumericQueryStepBuilderImpl<S, N, Long> {

    public NumericQueryRoundStepBuilder(final TraversalChainElement previous) {
        super(previous);
    }

    @Override
    public GraphTraversal<S, Long> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, N> traversal) {
        return traversal.map(traverser -> {
            if (traverser == null || traverser.get() == null) {
                // skip NULL values
                return null;
            }
            Number element = traverser.get();
            if (element == null) {
                return null;
            }
            return Math.round(element.doubleValue());
        });
    }
}
