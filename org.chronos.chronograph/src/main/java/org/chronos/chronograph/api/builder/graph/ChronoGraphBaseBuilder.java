package org.chronos.chronograph.api.builder.graph;

import java.io.File;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.internal.impl.engines.chunkdb.ChunkedChronoDB;
import org.chronos.chronodb.internal.impl.engines.inmemory.InMemoryChronoDB;
import org.chronos.chronodb.internal.impl.engines.jdbc.JdbcChronoDB;
import org.chronos.chronodb.internal.impl.engines.mapdb.MapDBChronoDB;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplChronoDB;
import org.chronos.chronograph.api.ChronoGraphFactory;
import org.chronos.chronograph.api.structure.ChronoGraph;

/**
 * This class acts as the first step in the fluent ChronoGraph builder API.
 *
 * <p>
 * You can get access to an instance of this class via {@link ChronoGraphFactory#create()}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoGraphBaseBuilder {

	/**
	 * Creates a new {@linkplain ChronoGraph} based on an {@linkplain InMemoryChronoDB in-memory} backend.
	 *
	 * @return The builder for in-memory graphs, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphInMemoryBuilder inMemoryGraph();

	/**
	 * Creates a new {@link ChronoGraph} based on a {@linkplain MapDBChronoDB MapDB} backend.
	 *
	 * @param workingFile
	 *            The file where the graph data is stored. Must not be <code>null</code>. Must refer to an existing file.
	 *
	 * @return The builder for MapDB graphs, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphMapDbBuilder mapDbGraph(File workingFile);

	/**
	 * Creates a {@link ChronoGraph} based on a {@linkplain TuplChronoDB TUPL} backend.
	 *
	 * @param workingFile
	 *            The file where the graph data is stored. Must not be <code>null</code>. Must refer to an existing file.
	 *
	 * @return The builder for TUPL graphs, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphTuplBuilder tuplGraph(File workingFile);

	/**
	 * Creates a {@link ChronoGraph} based on a {@linkplain JdbcChronoDB JDBC} backend.
	 *
	 * @param jdbcURL
	 *            The database URL to connect to via JDBC. Must not be <code>null</code>, must be a syntactically valid database connection URL.
	 *
	 * @return The builder for JDBC graphs, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphJdbcBuilder jdbcGraph(String jdbcURL);

	/**
	 * Creates a {@link ChronoGraph} based on a {@linkplain ChunkedChronoDB chunked} backend.
	 *
	 * @param workingFile
	 *            The root file for the database. Must not be <code>null</code>, must refer to an existing file.
	 *
	 * @return The builder for ChunkDB graphs, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphChunkDbBuilder chunkDbGraph(File workingFile);

	/**
	 * Creates a {@link ChronoGraph} based on a {@linkplain Properties properties} file.
	 *
	 * @param file
	 *            The properties file to read the graph configuration from. Must not be <code>null</code>, must refer to an existing properties file.
	 *
	 * @return The builder for the Graph, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphPropertyFileBuilder fromPropertiesFile(File file);

	/**
	 * Creates a {@link ChronoGraph} based on a {@linkplain Properties properties} file.
	 *
	 * @param filePath
	 *            The path to the properties file to read. Must not be <code>null</code>. Must refer to an existing file.
	 *
	 * @return The builder for the Graph, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphPropertyFileBuilder fromPropertiesFile(String filePath);

	/**
	 * Creates a {@link ChronoGraph} based on an Apache Commons {@link Configuration} object.
	 *
	 * @param configuration
	 *            The configuration to use for the new graph. Must not be <code>null</code>.
	 *
	 * @return The builder for the Graph, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphPropertyFileBuilder fromConfiguration(Configuration configuration);

	/**
	 * Creates a {@link ChronoGraph} based on a {@linkplain Properties properties} object.
	 *
	 * @param properties
	 *            The properties object to read the settings from. Must not be <code>null</code>.
	 *
	 * @return The builder for the Graph, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphPropertyFileBuilder fromProperties(Properties properties);

}
