package org.chronos.chronosphere.impl.query.steps.eobject;

import com.google.common.base.Objects;
import com.google.common.collect.SetMultimap;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.impl.query.EObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class EObjectQueryEGetInverseByNameStepBuilder<S> extends EObjectQueryStepBuilderImpl<S, Vertex> {

    private final String referenceName;

    public EObjectQueryEGetInverseByNameStepBuilder(final TraversalChainElement previous, String referenceName) {
        super(previous);
        checkNotNull(referenceName, "Precondition violation - argument 'referenceName' must not be NULL!");
        this.referenceName = referenceName;
    }

    @Override
    public GraphTraversal<S, Vertex> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        // get the registry
        ChronoEPackageRegistry registry = tx.getEPackageRegistry();
        // get the mapping from EClass to incoming EReferences
        SetMultimap<EClass, EReference> eClassToIncomingReferences = EMFUtils
            .eClassToIncomingEReferences(registry.getEPackages());
        // filter the references by name
        Set<EReference> eReferences = eClassToIncomingReferences.values().stream().filter(ref -> Objects.equal(ref.getName(), this.referenceName)).collect(Collectors.toSet());
        // generate an array of graph edge labels that correspond to these EReferences
        List<String> labelList = eReferences.stream().map(ref -> ChronoSphereGraphFormat.createReferenceEdgeLabel(registry, ref)).collect(Collectors.toList());
        String[] labels = labelList.toArray(new String[labelList.size()]);
        // navigate along the incoming edges
        return traversal.in(labels);
    }

}
