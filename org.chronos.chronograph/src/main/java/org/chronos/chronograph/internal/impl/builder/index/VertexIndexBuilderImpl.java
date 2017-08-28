package org.chronos.chronograph.internal.impl.builder.index;

import org.chronos.chronograph.api.builder.index.VertexIndexBuilder;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
import org.chronos.chronograph.internal.impl.index.ChronoGraphVertexIndex2;
import org.chronos.chronograph.internal.impl.index.IndexType;

public class VertexIndexBuilderImpl extends AbstractGraphElementIndexBuilder<VertexIndexBuilder>
		implements VertexIndexBuilder {

	protected VertexIndexBuilderImpl(final ChronoGraphIndexManagerInternal manager, final String propertyName, final IndexType indexType) {
		super(manager, propertyName, indexType);
	}

	@Override
	public ChronoGraphIndex build() {
		ChronoGraphVertexIndex2 index = new ChronoGraphVertexIndex2(this.propertyName, this.indexType);
		this.manager.addIndex(index);
		return index;
	}

}
