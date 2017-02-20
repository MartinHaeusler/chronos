package org.chronos.chronograph.internal.impl.builder.graph;

import java.io.File;

import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronograph.api.builder.graph.ChronoGraphChunkDbBuilder;

public class ChronoGraphChunkDbBuilderImpl extends AbstractChronoGraphFinalizableBuilder<ChronoGraphChunkDbBuilder> implements ChronoGraphChunkDbBuilder {

	public ChronoGraphChunkDbBuilderImpl(final File workingFile) {
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.CHUNKDB.toString());
		this.withProperty(ChronoDBConfiguration.WORK_FILE, workingFile.getAbsolutePath());
	}
}
