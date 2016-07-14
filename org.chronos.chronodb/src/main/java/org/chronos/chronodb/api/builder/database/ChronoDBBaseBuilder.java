package org.chronos.chronodb.api.builder.database;

import java.io.File;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;

/**
 * This is the starter interface for building a {@link ChronoDB} interface in a fluent API.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDBBaseBuilder {

	/**
	 * Loads the given file as a {@link Properties} file, and assigns it to a builder.
	 *
	 * <p>
	 * For details on the possible configurations, please refer to {@linkplain ChronoDBConfiguration the configuration
	 * API}.
	 *
	 * @param file
	 *            The file to load. Must exist, must be a file, must not be <code>null</code>.
	 * @return The builder to continue configuration with, with the properties from the given file loaded. Never
	 *         <code>null</code>.
	 */
	public ChronoDBPropertyFileBuilder fromPropertiesFile(File file);

	/**
	 * Loads the given file path as a {@link Properties} file, and assigns it to a builder.
	 *
	 * <p>
	 * For details on the possible configurations, please refer to {@linkplain ChronoDBConfiguration the configuration
	 * API}.
	 *
	 * @param filePath
	 *            The file path to load. Must exist, must be a file, must not be <code>null</code>.
	 * @return The builder to continue configuration with, with the properties from the given file loaded. Never
	 *         <code>null</code>.
	 */
	public ChronoDBPropertyFileBuilder fromPropertiesFile(String filePath);

	/**
	 * Loads the given Apache {@link Configuration} object and assigns it to a builder.
	 *
	 * <p>
	 * For details on the possible configurations, please refer to {@linkplain ChronoDBConfiguration the configuration
	 * API}.
	 *
	 * @param configuration
	 *            The configuration data to load. Must not be <code>null</code>.
	 * @return The builder to continue configuration with, with the properties from the configuration loaded. Never
	 *         <code>null</code>.
	 */
	public ChronoDBPropertyFileBuilder fromConfiguration(Configuration configuration);

	/**
	 * Loads the given {@link java.util.Properties} object and assigns it to a builder.
	 *
	 * <p>
	 * For details on the possible configurations, please refer to {@linkplain ChronoDBConfiguration the configuration
	 * API}.
	 *
	 * @param properties
	 *            The properties to load. Must not be <code>null</code>.
	 *
	 * @return The builder to continue configuration with, with the given properties loaded. Never <code>null</code>.
	 */
	public ChronoDBPropertyFileBuilder fromProperties(Properties properties);

	/**
	 * Creates a builder that is configured to instantiate an in-memory {@link ChronoDB}.
	 *
	 * <p>
	 * In-memory databases behave in the same way as regular ones, but are not persistent, i.e. their contents will be
	 * gone when the JVM shuts down, or the garbage collector reclaims their memory.
	 *
	 * @return The builder to continue configuration with. Never <code>null</code>.
	 */
	public ChronoDBInMemoryBuilder inMemoryDatabase();

	/**
	 * Creates a builder that is configured to instantiate a process-embedded, file-based {@link ChronoDB}.
	 *
	 * @param directory
	 *            The directory to write the DB data into. Must not be <code>null</code>, must point to a directory (not
	 *            a file).
	 * @return The builder to continue configuration with. Never <code>null</code>.
	 */
	public ChronoDBEmbeddedBuilder embeddedDatabase(File directory);

	/**
	 * Creates a builder that is configured to instantiate a {@link ChronoDB} that writes its contents to the SQL
	 * database at the given JDBC connection URL.
	 *
	 * @param jdbcConnectionURL
	 *            The JDBC connection URL to connect to. Must not be <code>null</code>, must be a valid JDBC URL.
	 * @return The builder to continue configuration with. Never <code>null</code>.
	 */
	public ChronoDBJdbcBuilder jdbcDatabase(String jdbcConnectionURL);

}
