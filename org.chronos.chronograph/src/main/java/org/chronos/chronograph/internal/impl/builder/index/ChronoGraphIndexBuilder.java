package org.chronos.chronograph.internal.impl.builder.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronograph.api.builder.index.EdgeIndexBuilder;
import org.chronos.chronograph.api.builder.index.IndexBuilderStarter;
import org.chronos.chronograph.api.builder.index.VertexIndexBuilder;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;

public class ChronoGraphIndexBuilder implements IndexBuilderStarter {

	private final ChronoGraphIndexManagerInternal manager;

	public ChronoGraphIndexBuilder(final ChronoGraphIndexManagerInternal manager) {
		checkNotNull(manager, "Precondition violation - argument 'manager' must not be NULL!");
		this.manager = manager;
	}

	@Override
	public VertexIndexBuilder onVertexProperty(final String propertyName) {
		return new VertexIndexBuilderImpl(this.manager, propertyName);
	}

	@Override
	public EdgeIndexBuilder onEdgeProperty(final String propertyName) {
		return new EdgeIndexBuilderImpl(this.manager, propertyName);
	}

}
