package org.chronos.chronosphere.test.base;

import static com.google.common.base.Preconditions.*;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronodb.test.base.AllBackendsTest;
import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.builder.repository.ChronoSphereInMemoryBuilder;
import org.chronos.chronosphere.api.builder.repository.ChronoSpherePropertyFileBuilder;
import org.chronos.chronosphere.internal.api.ChronoSphereInternal;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.logging.ChronoLogger;
import org.junit.After;

public abstract class AllChronoSphereBackendsTest extends AllBackendsTest {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private ChronoSphereInternal chronoSphere;

	// =====================================================================================================================
	// GETTERS & SETTERS
	// =====================================================================================================================

	protected ChronoSphereInternal getChronoSphere() {
		if (this.chronoSphere == null) {
			this.chronoSphere = this.instantiateChronoSphere(this.backend);
		}
		return this.chronoSphere;
	}

	// =====================================================================================================================
	// JUNIT CONTROL
	// =====================================================================================================================

	@After
	public void cleanUp() {
		ChronoLogger.logDebug("Closing ChronoSphere on backend '" + this.backend + "'.");
		if (this.chronoSphere != null && this.chronoSphere.isOpen()) {
			this.chronoSphere.close();
		}
	}

	// =====================================================================================================================
	// UTILITY
	// =====================================================================================================================

	protected ChronoSphereInternal reinstantiateChronoSphere() {
		ChronoLogger.logDebug("Reinstantiating ChronoSphere on backend '" + this.backend + "'.");
		if (this.chronoSphere != null && this.chronoSphere.isOpen()) {
			this.chronoSphere.close();
		}
		this.chronoSphere = this.instantiateChronoSphere(this.backend);
		return this.chronoSphere;
	}

	protected ChronoSphereInternal instantiateChronoSphere(final ChronosBackend backend) {
		checkNotNull(backend, "Precondition violation - argument 'backend' must not be NULL!");
		switch (backend) {
		case MAPDB:
			return (ChronoSphereInternal) this.createFileSphere();
		case INMEMORY:
			return (ChronoSphereInternal) this.createInMemorySphere();
		case JDBC:
			return (ChronoSphereInternal) this.createJdbcSphere();
		case TUPL:
			return (ChronoSphereInternal) this.createTuplSphere();
		case CHUNKDB:
			return (ChronoSphereInternal) this.createMetaDBSphere();
		default:
			throw new UnknownEnumLiteralException(backend);
		}
	}

	protected ChronoSphere createInMemorySphere() {
		ChronoSphereInMemoryBuilder builder = ChronoSphere.FACTORY.create().inMemoryRepository();
		this.applyExtraTestMethodProperties(builder);
		return builder.build();
	}

	protected ChronoSphere createFileSphere() {
		String path = this.createFileDBFile().getAbsolutePath();
		Configuration configuration = new BaseConfiguration();
		configuration.setProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.MAPDB.toString());
		configuration.setProperty(ChronoDBConfiguration.WORK_FILE, path);
		ChronoSpherePropertyFileBuilder builder = ChronoSphere.FACTORY.create().fromConfiguration(configuration);
		this.applyExtraTestMethodProperties(builder);
		return builder.build();
	}

	protected ChronoSphere createTuplSphere() {
		String path = this.createFileDBFile().getAbsolutePath();
		Configuration configuration = new BaseConfiguration();
		configuration.setProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.TUPL.toString());
		configuration.setProperty(ChronoDBConfiguration.WORK_FILE, path);
		ChronoSpherePropertyFileBuilder builder = ChronoSphere.FACTORY.create().fromConfiguration(configuration);
		this.applyExtraTestMethodProperties(builder);
		return builder.build();
	}

	protected ChronoSphere createJdbcSphere() {
		String jdbcURL = this.createJdbcDBConnectionURL();
		Configuration configuration = new BaseConfiguration();
		configuration.setProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.JDBC.toString());
		configuration.setProperty(ChronoDBConfiguration.JDBC_CONNECTION_URL, jdbcURL);
		ChronoSpherePropertyFileBuilder builder = ChronoSphere.FACTORY.create().fromConfiguration(configuration);
		this.applyExtraTestMethodProperties(builder);
		return builder.build();
	}

	protected ChronoSphere createMetaDBSphere() {
		String path = this.createFileDBFile().getAbsolutePath();
		Configuration configuration = new BaseConfiguration();
		configuration.setProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.CHUNKDB.toString());
		configuration.setProperty(ChronoDBConfiguration.WORK_FILE, path);
		ChronoSpherePropertyFileBuilder builder = ChronoSphere.FACTORY.create().fromConfiguration(configuration);
		this.applyExtraTestMethodProperties(builder);
		return builder.build();
	}

}
