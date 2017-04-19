package org.chronos.chronosphere.api.builder.repository;

import java.io.File;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;

public interface ChronoSphereBaseBuilder {

	public ChronoSphereInMemoryBuilder inMemoryRepository();

	public ChronoSpherePropertyFileBuilder fromPropertiesFile(File file);

	public ChronoSpherePropertyFileBuilder fromPropertiesFile(String filePath);

	public ChronoSpherePropertyFileBuilder fromConfiguration(Configuration configuration);

	public ChronoSpherePropertyFileBuilder fromProperties(Properties properties);

}
