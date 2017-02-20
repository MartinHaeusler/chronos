package org.chronos.chronograph.test.base;

import static com.google.common.base.Preconditions.*;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronodb.test.base.AllBackendsTest;
import org.chronos.chronograph.api.builder.graph.ChronoGraphInMemoryBuilder;
import org.chronos.chronograph.api.builder.graph.ChronoGraphPropertyFileBuilder;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.logging.ChronoLogger;
import org.junit.After;

public abstract class AllChronoGraphBackendsTest extends AllBackendsTest {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private ChronoGraph graph;

	// =================================================================================================================
	// GETTERS & SETTERS
	// =================================================================================================================

	protected ChronoGraph getGraph() {
		if (this.graph == null) {
			this.graph = this.instantiateChronoGraph(this.backend);
		}
		return this.graph;
	}

	// =================================================================================================================
	// JUNIT CONTROL
	// =================================================================================================================

	@After
	public void cleanUp() {
		if (this.graph != null && this.graph.isClosed() == false) {
			ChronoLogger.logDebug("Closing ChronoDB on backend '" + this.backend + "'.");
			this.graph.close();
		}
	}

	// =================================================================================================================
	// UTILITY
	// =================================================================================================================

	protected ChronoGraph reinstantiateGraph() {
		ChronoLogger.logDebug("Reinstantiating ChronoGraph on backend '" + this.backend + "'.");
		this.graph.close();
		this.graph = this.instantiateChronoGraph(this.backend);
		return this.graph;
	}

	protected ChronoGraph instantiateChronoGraph(final ChronosBackend backend) {
		checkNotNull(backend, "Precondition violation - argument 'backend' must not be NULL!");
		switch (backend) {
		case MAPDB:
			return this.createFileGraph();
		case INMEMORY:
			return this.createInMemoryGraph();
		case JDBC:
			return this.createJdbcGraph();
		case TUPL:
			return this.createTuplGraph();
		case CHUNKDB:
			return this.createMetaDBGraph();
		default:
			throw new UnknownEnumLiteralException(backend);
		}
	}

	protected ChronoGraph createInMemoryGraph() {
		ChronoGraphInMemoryBuilder builder = ChronoGraph.FACTORY.create().inMemoryGraph();
		this.applyExtraTestMethodProperties(builder);
		return builder.build();
	}

	protected Configuration createFileGraphConfiguration() {
		File file = this.createFileDBFile();
		return this.createFileGraphConfiguration(file);
	}

	protected Configuration createFileGraphConfiguration(final File dbFile) {
		checkNotNull(dbFile, "Precondition violation - argument 'dbFile' must not be NULL!");
		Configuration configuration = this.createFileDBConfiguration(dbFile);
		return configuration;
	}

	protected Configuration createTuplGraphConfiguration() {
		File file = this.createFileDBFile();
		return this.createTuplGraphConfiguration(file);
	}

	protected Configuration createTuplGraphConfiguration(final File dbFile) {
		checkNotNull(dbFile, "Precondition violation - argument 'dbFile' must not be NULL!");
		Configuration configuration = this.createTuplDBConfiguration(dbFile);
		return configuration;
	}

	protected Configuration createMetaDBGraphConfiguration() {
		File file = this.createFileDBFile();
		return this.createMetaDBGraphConfiguration(file);
	}

	protected Configuration createMetaDBGraphConfiguration(final File dbFile) {
		checkNotNull(dbFile, "Precondition violation - argument 'dbFile' must not be NULL!");
		Configuration configuration = this.createMetaDBConfiguration(dbFile);
		return configuration;
	}

	protected ChronoGraph createFileGraph() {
		Configuration configuration = this.createFileGraphConfiguration();
		return this.createGraph(configuration);
	}

	protected ChronoGraph createTuplGraph() {
		Configuration configuration = this.createTuplGraphConfiguration();
		return this.createGraph(configuration);
	}

	protected ChronoGraph createMetaDBGraph() {
		Configuration configuration = this.createMetaDBConfiguration();
		return this.createGraph(configuration);
	}

	protected Configuration createJdbcGraphConfiguration() {
		String connectionURL = this.createJdbcDBConnectionURL();
		return this.createJdbcGraphConfiguration(connectionURL);
	}

	protected Configuration createJdbcGraphConfiguration(final String connectionURL) {
		checkNotNull(connectionURL, "Precondition violation - argument 'connectionURL' must not be NULL!");
		Configuration configuration = this.createJdbcDBConfiguration(connectionURL);
		return configuration;
	}

	protected ChronoGraph createJdbcGraph() {
		Configuration configuration = this.createJdbcGraphConfiguration();
		return this.createGraph(configuration);
	}

	protected ChronoGraph createGraph(final Configuration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		ChronoGraphPropertyFileBuilder builder = ChronoGraph.FACTORY.create().fromConfiguration(configuration);
		this.applyExtraTestMethodProperties(builder);
		return builder.build();
	}

}
