package org.chronos.chronosphere.impl.query.steps.eobject;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.api.query.QueryStepBuilder;
import org.chronos.chronosphere.impl.query.EObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.eclipse.emf.ecore.EObject;

import static com.google.common.base.Preconditions.*;
import static org.chronos.chronosphere.impl.query.QueryUtils.*;

public class EObjectQueryAndStepBuilder<S> extends EObjectQueryStepBuilderImpl<S, Vertex> {

    private final QueryStepBuilder<EObject, ?>[] subqueries;

    @SafeVarargs
    public EObjectQueryAndStepBuilder(final TraversalChainElement previous, final QueryStepBuilder<EObject, ?>... subqueries) {
        super(previous);
        checkNotNull(subqueries, "Precondition violation - argument 'subqueries' must not be NULL!");
        this.subqueries = subqueries;
    }

    @Override
    public GraphTraversal<S, Vertex> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        return traversal.and(subQueriesToVertexTraversals(tx, this.subqueries, false));
    }
}
