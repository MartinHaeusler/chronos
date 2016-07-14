package org.chronos.chronodb.internal.impl.builder.database;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.api.builder.database.ChronoDBPropertyFileBuilder;

public class ChronoDBPropertyFileBuilderImpl extends AbstractChronoDBFinalizableBuilder<ChronoDBPropertyFileBuilder>
		implements ChronoDBPropertyFileBuilder {

	public ChronoDBPropertyFileBuilderImpl(final String propertiesFilePath) {
		checkNotNull(propertiesFilePath, "Precondition violation - argument 'propertiesFilePath' must not be NULL!");
		this.withPropertiesFile(propertiesFilePath);
	}

	public ChronoDBPropertyFileBuilderImpl(final File propertiesFile) {
		checkNotNull(propertiesFile, "Precondition violation - argument 'propertiesFile' must not be NULL!");
		this.withPropertiesFile(propertiesFile);
	}

	public ChronoDBPropertyFileBuilderImpl(final Configuration apacheConfiguration) {
		checkNotNull(apacheConfiguration, "Precondition violation - argument 'apacheConfiguration' must not be NULL!");
		Iterator<String> keyIterator = apacheConfiguration.getKeys();
		while (keyIterator.hasNext()) {
			String key = keyIterator.next();
			String value = String.valueOf(apacheConfiguration.getProperty(key));
			this.withProperty(key, value);
		}
	}
}
