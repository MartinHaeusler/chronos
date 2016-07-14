package org.chronos.chronodb.test.base;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.builder.database.ChronoDBPropertyFileBuilder;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.common.builder.ChronoBuilder;
import org.chronos.common.test.ChronosUnitTest;
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@RunWith(Parameterized.class)
public abstract class AllBackendsTest extends ChronosUnitTest {

	// =====================================================================================================================
	// JUNIT PARAMETERIZED TEST DATA
	// =====================================================================================================================

	@Parameters(name = "Using {0}")
	public static Collection<Object[]> data() {
		Set<Object[]> resultSet = Sets.newHashSet();
		Object[] inMemoryDB = new Object[] { ChronosBackend.INMEMORY };
		Object[] jdbcDB = new Object[] { ChronosBackend.JDBC };
		Object[] fileDB = new Object[] { ChronosBackend.FILE };
		resultSet.add(inMemoryDB);
		resultSet.add(jdbcDB);
		resultSet.add(fileDB);
		return resultSet;
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	@Parameter
	public ChronosBackend backend;

	// =====================================================================================================================
	// API
	// =====================================================================================================================

	protected String getChronoBackendName() {
		return this.backend.toString();
	}

	protected ChronoDB instantiateChronoDB(final ChronosBackend backend) {
		switch (backend) {
		case INMEMORY:
			return this.createInMemoryDB();
		case JDBC:
			return this.createJdbcDB();
		case FILE:
			return this.createFileDB();
		default:
			throw new RuntimeException("Unknown enumeration literal of ChronoDBBackend: '" + backend + "'!");
		}
	}

	protected String createJdbcDBConnectionURL() {
		Map<String, String> extraTestMethodProperties = this.getExtraTestMethodProperties();
		String connectionUrl = "jdbc:h2:mem:" + UUID.randomUUID().toString().replace("-", "");
		// check if we have a explicit connection URL
		if (extraTestMethodProperties.containsKey(ChronoDBConfiguration.JDBC_CONNECTION_URL)) {
			// override the default with the given URL
			String urlProperty = extraTestMethodProperties.get(ChronoDBConfiguration.JDBC_CONNECTION_URL);
			connectionUrl = urlProperty.replace("${testdir}", this.getTestDirectory().getAbsolutePath());
			// unify paths to use either forward or backward slashes, but not both
			if (File.separator.equals("\\")) {
				connectionUrl = connectionUrl.replaceAll("/", "\\\\");
			} else {
				connectionUrl = connectionUrl.replaceAll("\\\\", "/");
			}
		}
		return connectionUrl;
	}

	protected Configuration createJdbcDBConfiguration() {
		String connectionURL = this.createJdbcDBConnectionURL();
		return this.createJdbcDBConfiguration(connectionURL);
	}

	protected Configuration createJdbcDBConfiguration(final String connectionURL) {
		checkNotNull(connectionURL, "Precondition violation - argument 'connectionURL' must not be NULL!");
		Configuration configuration = new BaseConfiguration();
		configuration.addProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.JDBC.toString());
		configuration.addProperty(ChronoDBConfiguration.JDBC_CONNECTION_URL, connectionURL);
		return configuration;
	}

	protected ChronoDB createJdbcDB() {
		return this.createJdbcDB(this.createJdbcDBConnectionURL());
	}

	protected ChronoDB createJdbcDB(final String connectionURL) {
		checkNotNull(connectionURL, "Precondition violation - argument 'connectionURL' must not be NULL!");
		Configuration configuration = this.createJdbcDBConfiguration(connectionURL);
		return this.createDB(configuration);
	}

	protected Configuration createInMemoryDBConfiguration() {
		Configuration configuration = new BaseConfiguration();
		configuration.addProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.INMEMORY.toString());
		return configuration;
	}

	protected ChronoDB createInMemoryDB() {
		Configuration configuration = this.createInMemoryDBConfiguration();
		return this.createDB(configuration);
	}

	protected File createFileDBFile() {
		File testDirectory = this.getTestDirectory();
		File dbFile = new File(testDirectory, UUID.randomUUID().toString().replaceAll("-", "") + ".chronodb");
		try {
			dbFile.createNewFile();
		} catch (IOException e) {
			fail(e.toString());
		}
		return dbFile;
	}

	protected Configuration createFileDBConfiguration() {
		return this.createFileDBConfiguration(this.createFileDBFile());
	}

	protected Configuration createFileDBConfiguration(final File dbFile) {
		Configuration config = new BaseConfiguration();
		config.addProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.FILE.toString());
		config.addProperty(ChronoDBConfiguration.WORK_FILE, dbFile.getAbsolutePath());
		return config;
	}

	protected ChronoDB createFileDB() {
		return this.createFileDB(this.createFileDBFile());
	}

	protected ChronoDB createFileDB(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		Configuration configuration = this.createFileDBConfiguration(file);
		return this.createDB(configuration);
	}

	protected ChronoDB createDB(final Configuration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		ChronoDBPropertyFileBuilder builder = ChronoDB.FACTORY.create().fromConfiguration(configuration);
		this.applyExtraTestMethodProperties(builder);
		return builder.build();
	}

	// =====================================================================================================================
	// JUNIT CONTROL
	// =====================================================================================================================

	@Before
	public void checkOptOut() {
		DontRunWithBackend annotation = this.getClass().getAnnotation(DontRunWithBackend.class);
		if (annotation == null) {
			// we run on all backends
			return;
		}
		// we skip at least one backend; ensure that we are not running on that particular one
		for (ChronosBackend backend : annotation.value()) {
			// "assume" will cause a test to be skipped when the condition applies
			Assume.assumeFalse(backend.equals(this.backend));
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER MTHODS
	// =====================================================================================================================

	protected void applyExtraTestMethodProperties(final ChronoBuilder<?> builder) {
		Map<String, String> testMethodProperties = this.getExtraTestMethodProperties();
		// ALWAYS apply the debug property
		testMethodProperties.put(ChronoDBConfiguration.DEBUG, "true");
		for (Entry<String, String> entry : testMethodProperties.entrySet()) {
			String property = entry.getKey();
			String value = entry.getValue();
			if (property.equals(ChronoDBConfiguration.JDBC_CONNECTION_URL)) {
				// replace the test directory placeholder
				property = value.replace("${testdir}", this.getTestDirectory().getAbsolutePath());
				// unify paths to use either forward or backward slashes, but not both
				if (File.separator.equals("\\")) {
					property = property.replaceAll("/", "\\\\");
				} else {
					property = property.replaceAll("\\\\", "/");
				}
			}
			builder.withProperty(property, value);
		}
	}

	private Map<String, String> getExtraTestMethodProperties() {
		Map<String, String> properties = Maps.newHashMap();
		Method method = this.getCurrentTestMethod();
		if (method == null) {
			return properties;
		}
		InstantiateChronosWith[] annotations = method.getAnnotationsByType(InstantiateChronosWith.class);
		for (InstantiateChronosWith annotation : annotations) {
			String property = annotation.property();
			String value = annotation.value();
			properties.put(property, value);
		}
		return properties;
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	@Documented
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public static @interface DontRunWithBackend {

		public ChronosBackend[] value();

	}
}