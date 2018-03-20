package org.chronos.chronosphere.api.query;

import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;

public interface QueryStepBuilderInternal<S, E> extends QueryStepBuilder<S, E> {

    public TraversalChainElement getPrevious();

    void setPrevious(TraversalChainElement previous);
}
