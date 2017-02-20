package org.chronos.chronograph.api.builder.graph;

import java.io.File;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;

public interface ChronoGraphBaseBuilder {

	public ChronoGraphInMemoryBuilder inMemoryGraph();

	public ChronoGraphMapDbBuilder mapDbGraph(File workingFile);

	public ChronoGraphTuplBuilder tuplGraph(File workingFile);

	public ChronoGraphJdbcBuilder jdbcGraph(String jdbcURL);

	public ChronoGraphChunkDbBuilder chunkDbGraph(File workingFile);

	public ChronoGraphPropertyFileBuilder fromPropertiesFile(File file);

	public ChronoGraphPropertyFileBuilder fromPropertiesFile(String filePath);

	public ChronoGraphPropertyFileBuilder fromConfiguration(Configuration configuration);

	public ChronoGraphPropertyFileBuilder fromProperties(Properties properties);

}
