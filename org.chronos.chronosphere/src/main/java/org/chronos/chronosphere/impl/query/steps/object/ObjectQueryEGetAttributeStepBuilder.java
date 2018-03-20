package org.chronos.chronosphere.impl.query.steps.object;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.eclipse.emf.ecore.EAttribute;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

import static com.google.common.base.Preconditions.*;

public class ObjectQueryEGetAttributeStepBuilder<S> extends ObjectQueryStepBuilderImpl<S, Vertex, Object> {


    private final EAttribute eAttribute;

    public ObjectQueryEGetAttributeStepBuilder(final TraversalChainElement previous, final EAttribute eAttribute) {
        super(previous);
        checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
        this.eAttribute = eAttribute;
    }

    @Override
    public GraphTraversal<S, Object> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        ChronoEPackageRegistry registry = tx.getEPackageRegistry();
        return traversal.flatMap(this.eGetByEAttribute(this.eAttribute, registry));
    }

    @SuppressWarnings("unchecked")
    private <E2> Function<Traverser<Vertex>, Iterator<E2>> eGetByEAttribute(final EAttribute eAttribute, final ChronoEPackageRegistry registry) {
        return traverser -> {
            if (traverser == null || traverser.get() == null) {
                // skip NULL objects
                return Collections.emptyIterator();
            }
            Vertex vertex = traverser.get();
            String key = ChronoSphereGraphFormat.createVertexPropertyKey(registry, eAttribute);
            Object value = vertex.property(key).orElse(null);
            if (value == null) {
                return Collections.emptyIterator();
            } else if (value instanceof Collection) {
                return ((Collection<E2>) value).iterator();
            } else {
                return Iterators.singletonIterator((E2) value);
            }
        };
    }
}
