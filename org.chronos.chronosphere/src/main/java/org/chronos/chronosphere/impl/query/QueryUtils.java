package org.chronos.chronosphere.impl.query;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.api.query.EObjectQueryStepBuilder;
import org.chronos.chronosphere.api.query.QueryStepBuilder;
import org.chronos.chronosphere.api.query.QueryStepBuilderInternal;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.impl.query.steps.eobject.EObjectQueryAsEObjectStepBuilder;
import org.chronos.chronosphere.impl.query.steps.object.ObjectQueryEObjectReifyStepBuilder;
import org.chronos.chronosphere.impl.query.steps.object.ObjectQueryTerminalConverterStepBuilder;
import org.chronos.chronosphere.impl.query.traversal.TraversalBaseSource;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.impl.query.traversal.TraversalSource;
import org.chronos.chronosphere.impl.query.traversal.TraversalTransformer;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.eclipse.emf.ecore.EObject;

import java.util.List;
import java.util.ListIterator;
import java.util.function.Function;

public class QueryUtils {

    public static QueryStepBuilderInternal<?, ?> getFirstBuilderInChain(QueryStepBuilder<?, ?> builder) {
        QueryStepBuilderInternal<?, ?> current = (QueryStepBuilderInternal<?, ?>) builder;
        while (true) {
            TraversalChainElement previous = current.getPrevious();
            if (previous instanceof QueryStepBuilderInternal) {
                current = (QueryStepBuilderInternal<?, ?>) previous;
            } else {
                return current;
            }
        }
    }

    public static ChronoSphereTransactionInternal getTransactionFromTraversalBaseSourceOf(QueryStepBuilder<?, ?> builder) {
        TraversalChainElement current = (TraversalChainElement) builder;
        while (current instanceof QueryStepBuilderInternal) {
            TraversalChainElement previous = ((QueryStepBuilderInternal) current).getPrevious();
            current = previous;
        }
        if (current instanceof TraversalBaseSource) {
            TraversalBaseSource baseSource = (TraversalBaseSource) current;
            return baseSource.getTransaction();
        }
        throw new IllegalStateException("Calling a terminating method on a traversal which was created by a SubQuery method is invalid! Subqueries cannot be evaluated as standalone queries!");
    }

    @SuppressWarnings("unchecked")
    public static <S, E> GraphTraversal<S, E> resolveTraversalChain(QueryStepBuilder<S, E> builder, ChronoSphereTransactionInternal tx, boolean forceReifyEObjectsAtEnd) {
        List<TraversalChainElement> chainElements = Lists.newArrayList();
        TraversalChainElement currentBuilder = (TraversalChainElement) builder;
        while (currentBuilder != null) {
            chainElements.add(currentBuilder);
            if (currentBuilder instanceof QueryStepBuilderInternal) {
                QueryStepBuilderInternal stepBuilder = (QueryStepBuilderInternal) currentBuilder;
                currentBuilder = stepBuilder.getPrevious();
            } else {
                // end of chain: there is no previous element
                currentBuilder = null;
            }
        }
        chainElements = Lists.reverse(chainElements);
        if (chainElements.isEmpty()) {
            throw new IllegalStateException("At least one element must be on the query chain!");
        }
        TraversalChainElement first = chainElements.get(0);
        if (first instanceof TraversalSource == false) {
            throw new IllegalStateException("There is no traversal source for this query chain!");
        }
        TraversalSource source = (TraversalSource) first;

        optimizeTraversalChain(chainElements);


        if (forceReifyEObjectsAtEnd) {
            TraversalChainElement last = Iterables.getLast(chainElements);
            // if the last element of a chain is an EObjectQueryStep (i.e. it operates on vertices), then
            // we append a reify step to make sure that only EObjects exit the stream, not vertices.
            if (last instanceof EObjectQueryStepBuilder) {
                chainElements.add(new ObjectQueryTerminalConverterStepBuilder<>(last));
            }
        }

        GraphTraversal traversal = source.createTraversal();
        for (int i = 1; i < chainElements.size(); i++) {
            TraversalChainElement element = chainElements.get(i);
            if (element instanceof TraversalTransformer == false) {
                throw new IllegalStateException("Illegal traversal chain: there are multiple traversal sources!");
            }
            TraversalTransformer transformer = (TraversalTransformer) element;
            traversal = transformer.transformTraversal(tx, traversal);
        }
        return traversal;
    }

    private static void optimizeTraversalChain(final List<TraversalChainElement> chainElements) {
        // if we find the following sequence:
        // ReifyEObject (vertex to eobject) -> AsEObject (eobject as vertex)
        // ... we eliminate both steps because they cancel each other out.

        // also if we find the following sequence:
        // AsEObject (eObject as vertex) -> ReifyEObjects (vertex to eobject)
        // ... we eliminate both steps because they cancel each other out.

        ListIterator<TraversalChainElement> listIterator = chainElements.listIterator();
        while (listIterator.hasNext()) {
            TraversalChainElement current = listIterator.next();
            if (listIterator.hasNext() == false) {
                // this is the last element in our chain, we are done
                break;
            }
            TraversalChainElement next = listIterator.next();
            // move back after "peeking"
            listIterator.previous();
            if (current instanceof ObjectQueryEObjectReifyStepBuilder && next instanceof EObjectQueryAsEObjectStepBuilder) {
                // eliminate them both
                listIterator.previous();
                listIterator.remove();
                listIterator.next();
                listIterator.remove();
            } else if (current instanceof EObjectQueryAsEObjectStepBuilder && next instanceof ObjectQueryEObjectReifyStepBuilder) {
                // eliminate them both
                listIterator.previous();
                listIterator.remove();
                listIterator.next();
                listIterator.remove();
            }
        }

    }

    @SuppressWarnings("unchecked")
    public static GraphTraversal<Vertex, Object>[] subQueriesToVertexTraversals(final ChronoSphereTransactionInternal tx, final QueryStepBuilder<?, ?>[] subqueries, boolean forceReifyEObjectsAtEnd) {
        List<GraphTraversal<?, ?>> innerTraversals = Lists.newArrayList();
        for (QueryStepBuilder<?, ?> subquery : subqueries) {
            QueryStepBuilderInternal<?, ?> firstBuilder = getFirstBuilderInChain(subquery);
            if (firstBuilder instanceof EObjectQueryStepBuilder == false) {
                // the first step is NOT an EObject step, therefore it does NOT accept vertices.
                // In order for this subquery to be successful on a vertex stream input, we need
                // to prepend it with a step that reifies the Vertices in the stream into EObjects.
                TraversalSource<?, ?> source = (TraversalSource<?, ?>) firstBuilder.getPrevious();
                ObjectQueryEObjectReifyStepBuilder<Object> reifyStep = new ObjectQueryEObjectReifyStepBuilder<>(source);
                firstBuilder.setPrevious(reifyStep);
            }
            GraphTraversal<?, ?> traversal = resolveTraversalChain(subquery, tx, forceReifyEObjectsAtEnd);
            innerTraversals.add(traversal);
        }
        return innerTraversals.toArray((GraphTraversal<Vertex, Object>[]) new GraphTraversal[innerTraversals.size()]);
    }

    @SuppressWarnings("unchecked")
    public static GraphTraversal<Object, Object>[] subQueriesToObjectTraversals(final ChronoSphereTransactionInternal tx, final QueryStepBuilder<?, ?>[] subqueries, boolean forceReifyEObjectsAtEnd) {
        List<GraphTraversal<?, ?>> innerTraversals = Lists.newArrayList();
        for (QueryStepBuilder<?, ?> subquery : subqueries) {
            QueryStepBuilderInternal<?, ?> firstBuilder = getFirstBuilderInChain(subquery);
            if (firstBuilder instanceof EObjectQueryStepBuilder) {
                // the first step is an EObject step, therefore it does accept vertices.
                // In order for this subquery to be successful on an EObject stream input, we need
                // to prepend it with a step that transforms the EObjects in the stream into Vertices.
                TraversalSource<?, ?> source = (TraversalSource<?, ?>) firstBuilder.getPrevious();
                EObjectQueryAsEObjectStepBuilder<Object, EObject> transformStep = new EObjectQueryAsEObjectStepBuilder<>(source);
                firstBuilder.setPrevious(transformStep);
            }
            GraphTraversal<?, ?> traversal = resolveTraversalChain(subquery, tx, forceReifyEObjectsAtEnd);
            innerTraversals.add(traversal);
        }
        return innerTraversals.toArray((GraphTraversal<Object, Object>[]) new GraphTraversal[innerTraversals.size()]);
    }

    public static <S, C> GraphTraversal<S, C> castTraversalTo(GraphTraversal<S, ?> traversal, final Class<C> clazz) {
        return traversal.filter(t -> clazz.isInstance(t.get())).map(t -> clazz.cast(t.get()));
    }

    public static <S, C> GraphTraversal<S, C> castTraversalToNumeric(GraphTraversal<S, ?> traversal, final Function<Number, C> conversion) {
        return traversal.filter(t -> t.get() instanceof Number).map(t -> conversion.apply((Number) t.get()));
    }


    public static <S, E> GraphTraversal<S, E> prepareTerminalOperation(QueryStepBuilderInternal<S, E> builder, boolean forceReifyEObjectsAtEnd) {
        ChronoSphereTransactionInternal tx = getTransactionFromTraversalBaseSourceOf(builder);
        return resolveTraversalChain(builder, tx, forceReifyEObjectsAtEnd);
    }

    public static EObject mapVertexToEObject(final ChronoSphereTransaction tx, final Traverser<Vertex> traverser) {
        return mapVertexToEObject(tx, traverser.get());
    }

    public static EObject mapVertexToEObject(final ChronoSphereTransaction tx, final Vertex vertex) {
        if (vertex == null) {
            return null;
        }
        return tx.getEObjectById((String) vertex.id());
    }


    public static Vertex mapEObjectToVertex(final ChronoSphereTransaction tx, final EObject eObject) {
        ChronoGraph graph = ((ChronoSphereTransactionInternal) tx).getGraph();
        return mapEObjectToVertex(graph, eObject);
    }

    public static Vertex mapEObjectToVertex(final ChronoGraph graph, final EObject eObject) {
        if (eObject == null) {
            return null;
        }
        ChronoEObject cEObject = (ChronoEObject) eObject;
        if (graph == null) {
            throw new IllegalStateException("Graph is NULL!");
        }
        return Iterators.getOnlyElement(graph.vertices(cEObject.getId()), null);
    }


}
