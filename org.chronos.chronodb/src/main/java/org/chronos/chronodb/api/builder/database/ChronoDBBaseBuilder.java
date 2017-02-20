package org.chronos.chronodb.api.builder.database;

import java.io.File;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.MaintenanceManager;
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
	 *
	 * @deprecated This is an outdated builder method that now redirects to {@link #mapDbDatabase(File)}. This method
	 *             will eventually be deleted; prefer the explicit call to {@link #mapDbDatabase(File)} instead.
	 */
	@Deprecated
	public ChronoDBMapDBBuilder embeddedDatabase(File directory);

	/**
	 * Creates a builder that is configured to instantiate a process-embedded, <a href="http://www.mapdb.org/">MapDB</a>
	 * -based {@link ChronoDB}.
	 *
	 * @param file
	 *            The file to write the data into. Must not be <code>null</code>. Must point to an existing, accessible
	 *            file.
	 * @return The new builder to continue configuration with. Never <code>null</code>.
	 */
	public ChronoDBMapDBBuilder mapDbDatabase(File file);

	/**
	 * Creates a builder that is configured to instantiate a process-embedded,
	 * <a href="https://github.com/cojen/Tupl">TUPL</a>-based {@link ChronoDB}.
	 *
	 * @param file
	 *            The file to use as the "base path" for TUPL. All relevant data will be located in files that have the
	 *            path of the given file as prefix. Must point to an existing, accessible file.
	 * @return The new builder to continue configuration with. Never <code>null</code>.
	 */
	public ChronoDBTuplBuilder tuplDatabase(File file);

	/**
	 * Creates a builder that is configured to instantiate a process-embedded, chunked {@link ChronoDB}.
	 *
	 * <p>
	 * A chunked ChronoDB organizes the data in chunks and is capable of performing
	 * {@linkplain MaintenanceManager#performRolloverOnBranch(String) rollover} operations that allow for larger
	 * histories to be managed effectively.
	 *
	 * @param file
	 *            The base file where data is stored. Please note that this database backend will create several files
	 *            and folders next to the given base file; it is recommended to have a folder that contains nothing else
	 *            than this base file. Must not be <code>null</code>, must point to an existing, accessible file.
	 * @return The new builder to continue configuration with. Never <code>null</code>.
	 */
	public ChronoDBChunkedBuilder chunkedDatabase(File file);

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
