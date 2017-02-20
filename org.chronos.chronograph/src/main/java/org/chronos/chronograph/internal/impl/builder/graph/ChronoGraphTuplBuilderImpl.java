package org.chronos.chronograph.internal.impl.builder.graph;

import static com.google.common.base.Preconditions.*;

import java.io.File;

import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronograph.api.builder.graph.ChronoGraphTuplBuilder;

public class ChronoGraphTuplBuilderImpl extends AbstractChronoGraphFinalizableBuilder<ChronoGraphTuplBuilder> implements ChronoGraphTuplBuilder {

	public ChronoGraphTuplBuilderImpl(final File workingFile) {
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.TUPL.toString());
		this.withProperty(ChronoDBConfiguration.WORK_FILE, workingFile.getAbsolutePath());
	}

	@Override
	public ChronoGraphTuplBuilder withBackendCacheSize(final long backendCacheSize) {
		checkArgument(backendCacheSize > 0, "Precondition violation - argument 'backendCacheSize' must be > 0!");
		return this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND_CACHE, String.valueOf(backendCacheSize));
	}

}
