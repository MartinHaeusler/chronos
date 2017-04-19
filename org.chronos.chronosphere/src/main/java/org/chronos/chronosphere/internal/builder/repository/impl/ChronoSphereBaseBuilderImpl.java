package org.chronos.chronosphere.internal.builder.repository.impl;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.chronos.chronosphere.api.builder.repository.ChronoSphereBaseBuilder;
import org.chronos.chronosphere.api.builder.repository.ChronoSphereInMemoryBuilder;
import org.chronos.chronosphere.api.builder.repository.ChronoSpherePropertyFileBuilder;

public class ChronoSphereBaseBuilderImpl extends AbstractChronoSphereBuilder<ChronoSphereBaseBuilderImpl>
		implements ChronoSphereBaseBuilder {

	@Override
	public ChronoSphereInMemoryBuilder inMemoryRepository() {
		return new ChronoSphereInMemoryBuilderImpl();
	}

	@Override
	public ChronoSpherePropertyFileBuilder fromPropertiesFile(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkArgument(file.exists(), "Precondition violation - argument 'file' must refer to an existing file!");
		checkArgument(file.isFile(),
				"Precondition violation - argument 'file' must refer to a file (not a directory)!");
		return new ChronoSpherePropertyFileBuilderImpl(file);
	}

	@Override
	public ChronoSpherePropertyFileBuilder fromPropertiesFile(final String filePath) {
		checkNotNull(filePath, "Precondition violation - argument 'filePath' must not be NULL!");
		File file = new File(filePath);
		return this.fromPropertiesFile(file);
	}

	@Override
	public ChronoSpherePropertyFileBuilder fromConfiguration(final Configuration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		return new ChronoSpherePropertyFileBuilderImpl(configuration);
	}

	@Override
	public ChronoSpherePropertyFileBuilder fromProperties(final Properties properties) {
		checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
		Configuration configuration = new MapConfiguration(properties);
		return this.fromConfiguration(configuration);
	}

}
