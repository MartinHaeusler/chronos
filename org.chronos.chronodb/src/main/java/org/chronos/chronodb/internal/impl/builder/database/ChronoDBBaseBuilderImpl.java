package org.chronos.chronodb.internal.impl.builder.database;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.chronos.chronodb.api.builder.database.ChronoDBBaseBuilder;
import org.chronos.chronodb.api.builder.database.ChronoDBEmbeddedBuilder;
import org.chronos.chronodb.api.builder.database.ChronoDBInMemoryBuilder;
import org.chronos.chronodb.api.builder.database.ChronoDBJdbcBuilder;
import org.chronos.chronodb.api.builder.database.ChronoDBPropertyFileBuilder;
import org.chronos.chronodb.api.exceptions.ChronoDBConfigurationException;

public class ChronoDBBaseBuilderImpl extends AbstractChronoDBBuilder<ChronoDBBaseBuilderImpl>
		implements ChronoDBBaseBuilder {

	@Override
	public ChronoDBPropertyFileBuilder fromPropertiesFile(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		return new ChronoDBPropertyFileBuilderImpl(file);
	}

	@Override
	public ChronoDBPropertyFileBuilder fromPropertiesFile(final String filePath) {
		checkNotNull(filePath, "Precondition violation - argument 'filePath' must not be NULL!");
		return new ChronoDBPropertyFileBuilderImpl(filePath);
	}

	@Override
	public ChronoDBPropertyFileBuilder fromConfiguration(final Configuration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		return new ChronoDBPropertyFileBuilderImpl(configuration);
	}

	@Override
	public ChronoDBPropertyFileBuilder fromProperties(final Properties properties) {
		checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
		Configuration configuration = new MapConfiguration(properties);
		return this.fromConfiguration(configuration);
	}

	@Override
	public ChronoDBInMemoryBuilder inMemoryDatabase() {
		return new ChronoDBInMemoryBuilderImpl();
	}

	@Override
	public ChronoDBEmbeddedBuilder embeddedDatabase(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkArgument(file.isFile(), "Precondition violation - argument 'file' must be a file (not a directory)!");
		checkArgument(file.exists(),
				"Precondition violation - argument 'file' must exist, but does not! Searched here: '"
						+ file.getAbsolutePath() + "'");
		return new ChronoDBEmbeddedBuilderImpl(file);
	}

	@Override
	public ChronoDBJdbcBuilder jdbcDatabase(final String jdbcConnectionURL) {
		checkNotNull(jdbcConnectionURL, "Precondition violation - argument 'jdbcConnectionURL' must not be NULL!");
		try {
			Driver driver = DriverManager.getDriver(jdbcConnectionURL);
			if (driver == null) {
				throw new ChronoDBConfigurationException(
						"Could not find a suitable JDBC driver for URL '" + jdbcConnectionURL + "'!");
			}
		} catch (SQLException e) {
			throw new ChronoDBConfigurationException(
					"Could not find a suitable JDBC driver for URL '" + jdbcConnectionURL + "'!", e);
		}
		return new ChronoDBJdbcBuilderImpl(jdbcConnectionURL);
	}

}
