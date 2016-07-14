package org.chronos.chronodb.internal.impl.builder.database;

import static com.google.common.base.Preconditions.*;

import java.io.File;

import org.chronos.chronodb.api.builder.database.ChronoDBEmbeddedBuilder;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;

public class ChronoDBEmbeddedBuilderImpl extends AbstractChronoDBFinalizableBuilder<ChronoDBEmbeddedBuilder>
		implements ChronoDBEmbeddedBuilder {

	public ChronoDBEmbeddedBuilderImpl(final File directory) {
		checkNotNull(directory, "Precondition violation - argument 'directory' must not be NULL!");
		checkArgument(directory.isFile(),
				"Precondition violation - argument 'directory' must be a file (not a directory)!");
		checkArgument(directory.exists(),
				"Precondition violation - argument 'directory' must exist, but does not! Searched here: '"
						+ directory.getAbsolutePath() + "'");
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.FILE.toString());
		this.withProperty(ChronoDBConfiguration.WORK_FILE, directory.getAbsolutePath());
	}

	@Override
	public ChronoDBEmbeddedBuilder withDropOnShutdown() {
		return this.withProperty(ChronoDBConfiguration.DROP_ON_SHUTDOWN, "true");
	}

}
