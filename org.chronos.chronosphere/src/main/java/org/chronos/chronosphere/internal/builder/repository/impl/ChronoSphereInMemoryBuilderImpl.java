package org.chronos.chronosphere.internal.builder.repository.impl;

import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronosphere.api.builder.repository.ChronoSphereInMemoryBuilder;

public class ChronoSphereInMemoryBuilderImpl extends AbstractChronoSphereFinalizableBuilder<ChronoSphereInMemoryBuilder>
		implements ChronoSphereInMemoryBuilder {

	public ChronoSphereInMemoryBuilderImpl() {
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.INMEMORY.toString());
	}

}
