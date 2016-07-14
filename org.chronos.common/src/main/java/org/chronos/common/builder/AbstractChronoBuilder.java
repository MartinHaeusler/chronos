package org.chronos.common.builder;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.chronos.common.exceptions.ChronosException;

import com.google.common.collect.Maps;

public class AbstractChronoBuilder<SELF extends ChronoBuilder<?>> implements ChronoBuilder<SELF> {

	protected final Map<String, String> properties;

	public AbstractChronoBuilder() {
		this.properties = Maps.newHashMap();
	}

	@Override
	@SuppressWarnings("unchecked")
	public SELF withProperty(final String key, final String value) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		this.properties.put(key, value);
		return (SELF) this;
	}

	protected String getProperty(final String key) {
		return this.properties.get(key);
	}

	protected Map<String, String> getProperties() {
		return Collections.unmodifiableMap(Maps.newHashMap(this.properties));
	}

	protected Configuration getPropertiesAsConfiguration() {
		Map<String, String> properties = this.getProperties();
		Configuration config = new BaseConfiguration();
		for (Entry<String, String> entry : properties.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			config.setProperty(key, value);
		}
		return config;
	}

	@Override
	@SuppressWarnings("unchecked")
	public SELF withPropertiesFile(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkArgument(file.isFile(), "Precondition violation - argument 'file' must be a file (not a directory)!");
		checkArgument(file.exists(), "Precondition violation - argument 'file' must refer to an existing file!");
		checkArgument(file.getName().endsWith(".properties"),
				"Precondition violation - argument 'file' must specify a file name that ends with '.properties'!");
		Properties properties = new Properties();
		try {
			FileReader reader = new FileReader(file);
			properties.load(reader);
			for (Entry<Object, Object> entry : properties.entrySet()) {
				String key = String.valueOf(entry.getKey());
				String value = String.valueOf(entry.getValue());
				this.properties.put(key, value);
			}
			reader.close();
		} catch (IOException ioe) {
			throw new ChronosException("Failed to read properties file '" + file.getAbsolutePath() + "'!", ioe);
		}
		return (SELF) this;
	}

	@Override
	public SELF withPropertiesFile(final String filePath) {
		checkNotNull(filePath, "Precondition violation - argument 'filePath' must not be NULL!");
		File propertiesFile = new File(filePath);
		return this.withPropertiesFile(propertiesFile);
	}

}