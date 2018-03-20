package org.chronos.chronosphere.impl.query.steps.eobject;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.api.query.Direction;
import org.chronos.chronosphere.impl.query.EObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.eclipse.emf.ecore.EReference;

import static com.google.common.base.Preconditions.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class EObjectQueryClosureStepBuilder<S> extends EObjectQueryStepBuilderImpl<S, Vertex> {

    private final EReference eReference;
    private final Direction direction;

    public EObjectQueryClosureStepBuilder(final TraversalChainElement previous, final EReference eReference, Direction direction) {
        super(previous);
        checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
        checkNotNull(direction, "Precondition violation - argument 'direction' must not be NULL!");
        this.eReference = eReference;
        this.direction = direction;
    }

    @Override
    public GraphTraversal<S, Vertex> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        ChronoEPackageRegistry registry = tx.getEPackageRegistry();
        String label = ChronoSphereGraphFormat.createReferenceEdgeLabel(registry, this.eReference);
        GraphTraversal<Vertex, Vertex> closureTraversal;
        switch (this.direction) {
            case INCOMING:
                closureTraversal = in(label);
                break;
            case OUTGOING:
                closureTraversal = out(label);
                break;
            case BOTH:
                closureTraversal = both(label);
                break;
            default:
                throw new UnknownEnumLiteralException(this.direction);
        }
        return traversal.repeat(closureTraversal.simplePath()).emit().until(t -> false).dedup();
    }
}
