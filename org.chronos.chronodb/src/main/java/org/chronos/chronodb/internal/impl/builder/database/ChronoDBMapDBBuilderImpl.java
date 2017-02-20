package org.chronos.chronodb.internal.impl.builder.database;

import static com.google.common.base.Preconditions.*;

import java.io.File;

import org.chronos.chronodb.api.builder.database.ChronoDBMapDBBuilder;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;

public class ChronoDBMapDBBuilderImpl extends AbstractChronoDBFinalizableBuilder<ChronoDBMapDBBuilder>
		implements ChronoDBMapDBBuilder {

	public ChronoDBMapDBBuilderImpl(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkArgument(file.isFile(), "Precondition violation - argument 'file' must be a file (not a directory)!");
		checkArgument(file.exists(),
				"Precondition violation - argument 'file' must exist, but does not! Searched here: '"
						+ file.getAbsolutePath() + "'");
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.MAPDB.toString());
		this.withProperty(ChronoDBConfiguration.WORK_FILE, file.getAbsolutePath());
	}

}
