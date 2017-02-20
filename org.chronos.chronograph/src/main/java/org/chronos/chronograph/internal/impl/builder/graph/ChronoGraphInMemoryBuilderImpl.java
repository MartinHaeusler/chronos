package org.chronos.chronograph.internal.impl.builder.graph;

import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronograph.api.builder.graph.ChronoGraphInMemoryBuilder;

public class ChronoGraphInMemoryBuilderImpl extends AbstractChronoGraphFinalizableBuilder<ChronoGraphInMemoryBuilder>
		implements ChronoGraphInMemoryBuilder {

	public ChronoGraphInMemoryBuilderImpl() {
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.INMEMORY.toString());
	}

}
