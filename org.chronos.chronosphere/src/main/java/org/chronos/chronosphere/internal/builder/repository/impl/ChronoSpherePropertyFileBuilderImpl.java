package org.chronos.chronosphere.internal.builder.repository.impl;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.chronos.chronosphere.api.builder.repository.ChronoSpherePropertyFileBuilder;
import org.chronos.chronosphere.api.exceptions.ChronoSphereConfigurationException;

import com.google.common.collect.Sets;

public class ChronoSpherePropertyFileBuilderImpl
		extends AbstractChronoSphereFinalizableBuilder<ChronoSpherePropertyFileBuilder>
		implements ChronoSpherePropertyFileBuilder {

	public ChronoSpherePropertyFileBuilderImpl(final File propertiesFile) {
		checkNotNull(propertiesFile, "Precondition violation - argument 'propertiesFile' must not be NULL!");
		checkArgument(propertiesFile.exists(),
				"Precondition violation - argument 'propertiesFile' must refer to an existing file!");
		checkArgument(propertiesFile.isFile(),
				"Precondition violation - argument 'propertiesFile' must refer to a file (not a directory)!");
		try {
			Configuration configuration = new PropertiesConfiguration(propertiesFile);
			this.applyConfiguration(configuration);
		} catch (ConfigurationException e) {
			throw new ChronoSphereConfigurationException(
					"Failed to read properties file '" + propertiesFile.getAbsolutePath() + "'!", e);
		}
	}

	public ChronoSpherePropertyFileBuilderImpl(final Configuration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		try {
			this.applyConfiguration(configuration);
		} catch (Exception e) {
			throw new ChronoSphereConfigurationException("Failed to apply the given configuration'!", e);
		}
	}

	private void applyConfiguration(final Configuration configuration) {
		Set<String> keys = Sets.newHashSet(configuration.getKeys());
		for (String key : keys) {
			this.withProperty(key, configuration.getProperty(key).toString());
		}
	}

}
