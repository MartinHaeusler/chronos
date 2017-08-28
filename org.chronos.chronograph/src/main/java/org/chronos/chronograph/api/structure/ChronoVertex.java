package org.chronos.chronograph.api.structure;

import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * The {@link ChronoGraph}-specific representation of the general-purpose TinkerPop {@link Vertex} interface.
 *
 * <p>
 * This interface defines additional methods for all graph elements, mostly intended for internal use only.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoVertex extends Vertex, ChronoElement {

}