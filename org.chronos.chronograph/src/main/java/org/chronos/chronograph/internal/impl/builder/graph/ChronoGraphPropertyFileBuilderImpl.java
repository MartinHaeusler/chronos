package org.chronos.chronograph.internal.impl.builder.graph;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronograph.api.builder.graph.ChronoGraphPropertyFileBuilder;
import org.chronos.chronograph.api.exceptions.ChronoGraphConfigurationException;

import com.google.common.collect.Sets;

public class ChronoGraphPropertyFileBuilderImpl
		extends AbstractChronoGraphFinalizableBuilder<ChronoGraphPropertyFileBuilder>
		implements ChronoGraphPropertyFileBuilder {

	public ChronoGraphPropertyFileBuilderImpl(final File propertiesFile) {
		checkNotNull(propertiesFile, "Precondition violation - argument 'propertiesFile' must not be NULL!");
		checkArgument(propertiesFile.exists(),
				"Precondition violation - argument 'propertiesFile' must refer to an existing file!");
		checkArgument(propertiesFile.isFile(),
				"Precondition violation - argument 'propertiesFile' must refer to a file (not a directory)!");
		try {
			Configuration configuration = new PropertiesConfiguration(propertiesFile);
			this.applyConfiguration(configuration);
		} catch (ConfigurationException e) {
			throw new ChronoGraphConfigurationException(
					"Failed to read properties file '" + propertiesFile.getAbsolutePath() + "'!", e);
		}
	}

	public ChronoGraphPropertyFileBuilderImpl(final Configuration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		try {
			this.applyConfiguration(configuration);
		} catch (Exception e) {
			throw new ChronoGraphConfigurationException("Failed to apply the given configuration'!", e);
		}
	}

	private void applyConfiguration(final Configuration configuration) {
		Set<String> keys = Sets.newHashSet(configuration.getKeys());
		for (String key : keys) {
			this.withProperty(key, configuration.getProperty(key).toString());
		}
		// in ChronoGraph, we can ALWAYS ensure immutability of ChronoDB cache values. The reason for this is
		// that ChronoGraph only passes records (e.g. VertexRecord) to the underlying ChronoDB, and records
		// are always immutable.
		this.withProperty(ChronoDBConfiguration.ASSUME_CACHE_VALUES_ARE_IMMUTABLE, "true");
	}

}
