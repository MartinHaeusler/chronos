package org.chronos.chronodb.internal.impl.builder.database;

import org.chronos.chronodb.api.builder.database.ChronoDBInMemoryBuilder;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;

public class ChronoDBInMemoryBuilderImpl extends AbstractChronoDBFinalizableBuilder<ChronoDBInMemoryBuilder>
		implements ChronoDBInMemoryBuilder {

	public ChronoDBInMemoryBuilderImpl() {
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.INMEMORY.toString());
	}

}
