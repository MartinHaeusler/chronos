package org.chronos.chronosphere.impl.query.steps.eobject;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.api.query.EObjectQueryStepBuilder;
import org.chronos.chronosphere.api.query.QueryStepBuilder;
import org.chronos.chronosphere.api.query.QueryStepBuilderInternal;
import org.chronos.chronosphere.impl.query.EObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.QueryUtils;
import org.chronos.chronosphere.impl.query.steps.object.ObjectQueryEObjectReifyStepBuilder;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.impl.query.traversal.TraversalSource;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.eclipse.emf.ecore.EObject;

import static com.google.common.base.Preconditions.*;
import static org.chronos.chronosphere.impl.query.QueryUtils.*;

public class EObjectQueryNotStepBuilder<S> extends EObjectQueryStepBuilderImpl<S, Vertex> {

    private final QueryStepBuilder<EObject, ?> subquery;

    public EObjectQueryNotStepBuilder(final TraversalChainElement previous, final QueryStepBuilder<EObject, ?> subquery) {
        super(previous);
        checkNotNull(subquery, "Precondition violation - argument 'subquery' must not be NULL!");
        this.subquery = subquery;
        QueryStepBuilderInternal<?, ?> firstBuilderInChain = QueryUtils.getFirstBuilderInChain(subquery);
        if (firstBuilderInChain instanceof EObjectQueryStepBuilder == false) {
            // prepend a "reify" step to make the subqueries compatible with this query
            TraversalSource<?, ?> source = (TraversalSource<?, ?>) firstBuilderInChain.getPrevious();
            firstBuilderInChain.setPrevious(new ObjectQueryEObjectReifyStepBuilder<>(source));
        }
    }

    @Override
    public GraphTraversal<S, Vertex> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        // apply the transforms on the subquery. We can hand over the vertex
        // representations here directly because we made sure above that the
        // incoming chain has an EObject query step as its first step.
        GraphTraversal innerTraversal = resolveTraversalChain(this.subquery, tx, false);
        return traversal.not(innerTraversal);
    }

}
