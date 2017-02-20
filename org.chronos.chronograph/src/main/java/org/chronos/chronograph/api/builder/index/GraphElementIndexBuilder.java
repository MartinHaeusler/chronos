package org.chronos.chronograph.api.builder.index;

import org.chronos.chronograph.api.index.ChronoGraphIndex;

public interface GraphElementIndexBuilder<SELF extends GraphElementIndexBuilder<SELF>> {

	public ChronoGraphIndex build();

}
