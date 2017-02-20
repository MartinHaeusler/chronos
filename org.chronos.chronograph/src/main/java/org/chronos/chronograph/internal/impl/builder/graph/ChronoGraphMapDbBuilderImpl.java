package org.chronos.chronograph.internal.impl.builder.graph;

import java.io.File;

import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronograph.api.builder.graph.ChronoGraphMapDbBuilder;

public class ChronoGraphMapDbBuilderImpl extends AbstractChronoGraphFinalizableBuilder<ChronoGraphMapDbBuilder> implements ChronoGraphMapDbBuilder {

	public ChronoGraphMapDbBuilderImpl(final File workingFile) {
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.MAPDB.toString());
		this.withProperty(ChronoDBConfiguration.WORK_FILE, workingFile.getAbsolutePath());
	}

}
