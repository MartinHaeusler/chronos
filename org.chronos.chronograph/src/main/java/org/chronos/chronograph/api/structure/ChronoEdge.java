package org.chronos.chronograph.api.structure;

import org.apache.tinkerpop.gremlin.structure.Edge;

/**
 * The {@link ChronoGraph}-specific representation of the general-purpose TinkerPop {@link Edge} interface.
 *
 * <p>
 * This interface defines additional methods for all graph elements, mostly intended for internal use only.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoEdge extends Edge, ChronoElement {

}