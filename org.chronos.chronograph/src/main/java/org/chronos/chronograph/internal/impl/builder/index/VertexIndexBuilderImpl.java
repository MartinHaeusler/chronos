package org.chronos.chronograph.internal.impl.builder.index;

import org.chronos.chronograph.api.builder.index.VertexIndexBuilder;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
import org.chronos.chronograph.internal.impl.index.ChronoGraphVertexIndex;

public class VertexIndexBuilderImpl extends AbstractGraphElementIndexBuilder<VertexIndexBuilder>
		implements VertexIndexBuilder {

	protected VertexIndexBuilderImpl(final ChronoGraphIndexManagerInternal manager, final String propertyName) {
		super(manager, propertyName);
	}

	@Override
	public ChronoGraphIndex build() {
		ChronoGraphVertexIndex index = new ChronoGraphVertexIndex(this.propertyName);
		this.manager.addIndex(index);
		return index;
	}

}
