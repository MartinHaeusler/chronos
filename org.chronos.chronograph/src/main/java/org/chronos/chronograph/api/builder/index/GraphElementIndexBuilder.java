package org.chronos.chronograph.api.builder.index;

import org.chronos.chronograph.api.index.ChronoGraphIndex;

/**
 * A step in the fluent graph index builder API.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @param <SELF>
 *            The actual type of <code>this</code>.
 */
public interface GraphElementIndexBuilder<SELF extends GraphElementIndexBuilder<SELF>> {

	public ChronoGraphIndex build();

}
