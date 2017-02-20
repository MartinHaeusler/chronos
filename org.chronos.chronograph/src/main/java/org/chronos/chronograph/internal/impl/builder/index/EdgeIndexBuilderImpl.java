package org.chronos.chronograph.internal.impl.builder.index;

import org.chronos.chronograph.api.builder.index.EdgeIndexBuilder;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
import org.chronos.chronograph.internal.impl.index.ChronoGraphEdgeIndex;

public class EdgeIndexBuilderImpl extends AbstractGraphElementIndexBuilder<EdgeIndexBuilder>
		implements EdgeIndexBuilder {

	protected EdgeIndexBuilderImpl(final ChronoGraphIndexManagerInternal manager, final String propertyName) {
		super(manager, propertyName);
	}

	@Override
	public ChronoGraphIndex build() {
		ChronoGraphEdgeIndex index = new ChronoGraphEdgeIndex(this.propertyName);
		this.manager.addIndex(index);
		return index;
	}

}
