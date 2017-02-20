package org.chronos.chronodb.internal.impl.builder.database;

import static com.google.common.base.Preconditions.*;

import java.io.File;

import org.chronos.chronodb.api.builder.database.ChronoDBTuplBuilder;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;

public class ChronoDBTuplBuilderImpl extends AbstractChronoDBFinalizableBuilder<ChronoDBTuplBuilder>
		implements ChronoDBTuplBuilder {

	public ChronoDBTuplBuilderImpl(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkArgument(file.isFile(), "Precondition violation - argument 'file' must be a file (not a directory)!");
		checkArgument(file.exists(),
				"Precondition violation - argument 'directory' must exist, but does not! Searched here: '"
						+ file.getAbsolutePath() + "'");
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.TUPL.toString());
		this.withProperty(ChronoDBConfiguration.WORK_FILE, file.getAbsolutePath());
	}

	@Override
	public ChronoDBTuplBuilder withBackendCacheOfSizeInBytes(final long backendCacheSizeBytes) {
		checkArgument(backendCacheSizeBytes > 0,
				"Precondition violation - argument 'backendCacheSizeBytes' must be strictly greater than zero!");
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND_CACHE, String.valueOf(backendCacheSizeBytes));
		return this;
	}

}
