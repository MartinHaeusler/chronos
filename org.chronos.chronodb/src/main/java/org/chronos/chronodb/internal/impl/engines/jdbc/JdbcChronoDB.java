package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.MaintenanceManager;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.api.exceptions.ChronoDBConfigurationException;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.api.exceptions.ChronosBuildVersionConflictException;
import org.chronos.chronodb.internal.api.BranchManagerInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.api.query.QueryManager;
import org.chronos.chronodb.internal.impl.cache.bogus.ChronoDBBogusCache;
import org.chronos.chronodb.internal.impl.cache.mosaic.MosaicCache;
import org.chronos.chronodb.internal.impl.engines.base.AbstractChronoDB;
import org.chronos.chronodb.internal.impl.engines.inmemory.InMemorySerializationManager;
import org.chronos.chronodb.internal.impl.index.DocumentBasedIndexManager;
import org.chronos.chronodb.internal.impl.query.StandardQueryManager;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.version.ChronosVersion;

import com.mchange.v2.c3p0.DataSources;

/**
 * This implementation of {@link ChronoDB} utilizes a JDBC connection to a relational database for storage purposes.
 *
 * <p>
 * An instance of this class can be created using the {@link ChronoDB#FACTORY}:
 *
 * <pre>
 * ChronoDB db = ChronoDB.FACTORY.build().createJdbcDatabase(&quot;jdbc:mysql...&quot;).create();
 * </pre>
 *
 * <p>
 * <b>Performance note:</b><br>
 * This implementation will likely not scale as well as the NoSQL-based performance backends with respect to runtime, in particular if the relational database needs to be accessed via network.
 *
 *
 * <p>
 * <b>Implementation notes</b><br>
 * This implementation makes use of several tables:
 * <ul>
 * <li><b>Navigation Table</b><br>
 * This table serves for navigation purposes. It relates branch names and keyspace names to the tables in which this particular branch/keyspace combination is stored. For more details, see {@link JdbcNavigationTable}. Results retrieved from this table are cached internally to maximize the runtime efficiency. There is exactly one Navigation Table per ChronoDB instance.
 *
 * <li><b>Time Table</b><br>
 * The time table serves the purpose of storing the "now" timestamp for each keyspace. This table is updated on each successful commit. In contrast to other tables, this table may be subject to <code>UPDATE</code> statements. For details, please refer to {@link JdbcTimeTable}. There is exactly one Time Table per ChronoDB instance.
 *
 * <li><b>Matrix Tables</b><br>
 * Matrix tables are used for the actual key-value storage. These tables are strictly append-only. There can be multiple matrix tables per ChronoDB. For details, please refer to {@link JdbcMatrixTable}.
 * </ul>
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class JdbcChronoDB extends AbstractChronoDB {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final DataSource dataSource;

	private final BranchManagerInternal branchManager;
	private final SerializationManager serializationManager;
	private final IndexManager indexManager;
	private final QueryManager queryManager;
	private final MaintenanceManager maintenanceManager;

	private final ChronoDBCache cache;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public JdbcChronoDB(final ChronoDBConfiguration configuration) {
		super(configuration);
		this.dataSource = this.createDataSource(configuration);
		this.branchManager = new JdbcBranchManager(this);
		// we use the in-memory solution here; when we allow serialization
		// customization and configuration, we must use a solution that persists
		// the configured version in the database.
		this.serializationManager = new InMemorySerializationManager();
		this.indexManager = new DocumentBasedIndexManager(this, new JdbcIndexManagerBackend(this));
		this.queryManager = new StandardQueryManager(this);
		this.maintenanceManager = new JdbcMaintenanceManager(this);
		if (configuration.isCachingEnabled()) {
			this.cache = new MosaicCache(configuration.getCacheMaxSize());
		} else {
			this.cache = new ChronoDBBogusCache();
		}
		this.initializeShutdownHook();
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public BranchManagerInternal getBranchManager() {
		return this.branchManager;
	}

	@Override
	public SerializationManager getSerializationManager() {
		return this.serializationManager;
	}

	@Override
	public IndexManager getIndexManager() {
		return this.indexManager;
	}

	@Override
	public QueryManager getQueryManager() {
		return this.queryManager;
	}

	@Override
	public MaintenanceManager getMaintenanceManager() {
		return this.maintenanceManager;
	}

	@Override
	public ChronoDBCache getCache() {
		return this.cache;
	}

	@Override
	public boolean isFileBased() {
		return false;
	}

	@Override
	public void updateChronosVersionTo(final ChronosVersion chronosVersion) {
		checkNotNull(chronosVersion, "Precondition violation - argument 'chronosVersion' must not be NULL!");
		try (Connection connection = this.getDataSource().getConnection()) {
			JdbcChronosBuildVersionTable table = JdbcChronosBuildVersionTable.get(connection);
			table.ensureExists();
			table.setChronosVersion(chronosVersion.toString());
			connection.commit();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to update Chronos Build Version in database!", e);
		}
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	protected DataSource getDataSource() {
		return this.dataSource;
	}

	@Override
	protected void updateBuildVersionInDatabase() {
		try (Connection connection = this.getDataSource().getConnection()) {
			JdbcChronosBuildVersionTable table = JdbcChronosBuildVersionTable.get(connection);
			table.ensureExists();
			String dbVersionString = table.getChronosVersion();
			if (dbVersionString == null) {
				// we do not have a version stored in the db; store our current version
				table.setChronosVersion(ChronosVersion.getCurrentVersion().toString());
			} else {
				// check if the stored version is newer than our current version
				ChronosVersion dbVersion = ChronosVersion.parse(dbVersionString);
				ChronosVersion currentVersion = ChronosVersion.getCurrentVersion();
				if (dbVersion.compareTo(currentVersion) > 0) {
					// the database has been written by a NEWER version of chronos; we might be incompatible
					if (currentVersion.isReadCompatibleWith(dbVersion)) {
						ChronoLogger.logWarning("The database was written by Chronos '" + dbVersion.toString()
								+ "', but this is the older version '" + currentVersion.toString()
								+ "'! Some features may be unsupported by this older version. We strongly recommend updating Chronos to version '"
								+ dbVersion + "' or higher for working with this database!");
						return;
					} else {
						// the current chronos version is not read-compatible with the (newer) version that created this
						// database; we must not touch it
						throw new ChronosBuildVersionConflictException("The database was written by Chronos '"
								+ dbVersion.toString() + "', but this is the older version '"
								+ ChronosVersion.getCurrentVersion().toString()
								+ "'! Older versions of Chronos cannot open databases created by newer versions!");
					}
				} else {
					// database was created by an older version of chronos; upate it
					table.setChronosVersion(currentVersion.toString());
				}
			}
			connection.commit();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to update Chronos Build Version in database!", e);
		}
	}

	private void initializeShutdownHook() {
		this.addShutdownHook(() -> {
			try {
				DataSources.destroy(this.dataSource);
			} catch (SQLException e) {
				throw new ChronoDBStorageBackendException("Failed to close the database", e);
			}
		});
	}

	private DataSource createDataSource(final ChronoDBConfiguration config) {
		String jdbcURL = config.getJdbcConnectionUrl();
		String username = config.getJdbcCredentialsUsername();
		String password = config.getJdbcCredentialsPassword();
		try {
			DataSource unpooledDS = null;
			if (username != null && password != null) {
				unpooledDS = DataSources.unpooledDataSource(jdbcURL, username, password);
			} else {
				unpooledDS = DataSources.unpooledDataSource(jdbcURL);
			}
			DataSource pooledDS = DataSources.pooledDataSource(unpooledDS);
			return pooledDS;
		} catch (SQLException e) {
			throw new ChronoDBConfigurationException("Could not connect to the given SQL Database!", e);
		}
	}

}
