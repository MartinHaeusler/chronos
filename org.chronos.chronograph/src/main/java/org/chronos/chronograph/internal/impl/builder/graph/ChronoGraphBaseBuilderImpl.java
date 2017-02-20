package org.chronos.chronograph.internal.impl.builder.graph;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.chronos.chronograph.api.builder.graph.ChronoGraphBaseBuilder;
import org.chronos.chronograph.api.builder.graph.ChronoGraphChunkDbBuilder;
import org.chronos.chronograph.api.builder.graph.ChronoGraphInMemoryBuilder;
import org.chronos.chronograph.api.builder.graph.ChronoGraphJdbcBuilder;
import org.chronos.chronograph.api.builder.graph.ChronoGraphMapDbBuilder;
import org.chronos.chronograph.api.builder.graph.ChronoGraphPropertyFileBuilder;
import org.chronos.chronograph.api.builder.graph.ChronoGraphTuplBuilder;

public class ChronoGraphBaseBuilderImpl extends AbstractChronoGraphBuilder<ChronoGraphBaseBuilderImpl> implements ChronoGraphBaseBuilder {

	@Override
	public ChronoGraphInMemoryBuilder inMemoryGraph() {
		return new ChronoGraphInMemoryBuilderImpl();
	}

	@Override
	public ChronoGraphMapDbBuilder mapDbGraph(final File workingFile) {
		checkNotNull(workingFile, "Precondition violation - argument 'workingFile' must not be NULL!");
		checkArgument(workingFile.exists(), "Precondition violation - argument 'workingFile' must exist!");
		checkArgument(workingFile.isFile(), "Precondition violation - argument 'workingFile' must point to a file (not a directory)!");
		checkArgument(workingFile.canRead(), "Precondition violation - argument 'workingFile' refers to a non-readable file (permission issue)!");
		checkArgument(workingFile.canWrite(), "Precondition violation - argument 'workingFile' refers to a non-writable file (permission issue)!");
		return new ChronoGraphMapDbBuilderImpl(workingFile);
	}

	@Override
	public ChronoGraphTuplBuilder tuplGraph(final File workingFile) {
		checkNotNull(workingFile, "Precondition violation - argument 'workingFile' must not be NULL!");
		checkArgument(workingFile.exists(), "Precondition violation - argument 'workingFile' must exist!");
		checkArgument(workingFile.isFile(), "Precondition violation - argument 'workingFile' must point to a file (not a directory)!");
		checkArgument(workingFile.canRead(), "Precondition violation - argument 'workingFile' refers to a non-readable file (permission issue)!");
		checkArgument(workingFile.canWrite(), "Precondition violation - argument 'workingFile' refers to a non-writable file (permission issue)!");
		return new ChronoGraphTuplBuilderImpl(workingFile);
	}

	@Override
	public ChronoGraphJdbcBuilder jdbcGraph(final String jdbcURL) {
		checkNotNull(jdbcURL, "Precondition violation - argument 'jdbcURL' must not be NULL!");
		return new ChronoGraphJdbcBuilderImpl(jdbcURL);
	}

	@Override
	public ChronoGraphChunkDbBuilder chunkDbGraph(final File workingFile) {
		checkNotNull(workingFile, "Precondition violation - argument 'workingFile' must not be NULL!");
		checkArgument(workingFile.exists(), "Precondition violation - argument 'workingFile' must exist!");
		checkArgument(workingFile.isFile(), "Precondition violation - argument 'workingFile' must point to a file (not a directory)!");
		checkArgument(workingFile.canRead(), "Precondition violation - argument 'workingFile' refers to a non-readable file (permission issue)!");
		checkArgument(workingFile.canWrite(), "Precondition violation - argument 'workingFile' refers to a non-writable file (permission issue)!");
		return new ChronoGraphChunkDbBuilderImpl(workingFile);
	}

	@Override
	public ChronoGraphPropertyFileBuilder fromPropertiesFile(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkArgument(file.exists(), "Precondition violation - argument 'file' must refer to an existing file!");
		checkArgument(file.isFile(), "Precondition violation - argument 'file' must refer to a file (not a directory)!");
		return new ChronoGraphPropertyFileBuilderImpl(file);
	}

	@Override
	public ChronoGraphPropertyFileBuilder fromConfiguration(final Configuration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		return new ChronoGraphPropertyFileBuilderImpl(configuration);
	}

	@Override
	public ChronoGraphPropertyFileBuilder fromProperties(final Properties properties) {
		checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
		Configuration configuration = new MapConfiguration(properties);
		return this.fromConfiguration(configuration);
	}

	@Override
	public ChronoGraphPropertyFileBuilder fromPropertiesFile(final String filePath) {
		checkNotNull(filePath, "Precondition violation - argument 'filePath' must not be NULL!");
		File file = new File(filePath);
		return this.fromPropertiesFile(file);
	}

}
