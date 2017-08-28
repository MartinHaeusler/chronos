package org.chronos.chronograph.internal.impl.builder.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronograph.api.builder.index.EdgeIndexBuilder;
import org.chronos.chronograph.api.builder.index.ElementTypeChoiceIndexBuilder;
import org.chronos.chronograph.api.builder.index.VertexIndexBuilder;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
import org.chronos.chronograph.internal.impl.index.IndexType;

public class ElementTypeChoiceIndexBuilderImpl implements ElementTypeChoiceIndexBuilder {

	private final ChronoGraphIndexManagerInternal manager;
	private final IndexType indexType;

	public ElementTypeChoiceIndexBuilderImpl(final ChronoGraphIndexManagerInternal manager, final IndexType indexType) {
		checkNotNull(manager, "Precondition violation - argument 'manager' must not be NULL!");
		checkNotNull(indexType, "Precondition violation - argument 'indexType' must not be NULL!");
		this.manager = manager;
		this.indexType = indexType;
	}

	@Override
	public VertexIndexBuilder onVertexProperty(final String propertyName) {
		return new VertexIndexBuilderImpl(this.manager, propertyName, this.indexType);
	}

	@Override
	public EdgeIndexBuilder onEdgeProperty(final String propertyName) {
		return new EdgeIndexBuilderImpl(this.manager, propertyName, this.indexType);
	}
}
