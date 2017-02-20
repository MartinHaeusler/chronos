package org.chronos.chronodb.internal.impl.engines.tupl;

import java.io.File;

import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.MaintenanceManager;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.api.exceptions.ChronosBuildVersionConflictException;
import org.chronos.chronodb.internal.api.BranchManagerInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.api.query.QueryManager;
import org.chronos.chronodb.internal.impl.engines.base.AbstractChronoDB;
import org.chronos.chronodb.internal.impl.engines.inmemory.InMemorySerializationManager;
import org.chronos.chronodb.internal.impl.index.DocumentBasedIndexManager;
import org.chronos.chronodb.internal.impl.query.StandardQueryManager;
import org.chronos.common.exceptions.ChronosIOException;
import org.chronos.common.version.ChronosVersion;
import org.cojen.tupl.Database;
import org.cojen.tupl.Transaction;

public class TuplChronoDB extends AbstractChronoDB {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	public static final String MANAGEMENT_INDEX_NAME = "chronosManagement";
	public static final String MANAGEMENT_INDEX__CHRONOS_BUILD_VERSION = "chronos.buildVersion";

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final Database database;

	private final SerializationManager serializationManager;
	private final IndexManager indexManager;
	private final BranchManagerInternal branchManager;
	private final QueryManager queryManager;
	private final MaintenanceManager maintenanceManager;

	private final ChronoDBCache cache;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public TuplChronoDB(final ChronoDBConfiguration configuration) {
		super(configuration);
		File workingFile = configuration.getWorkingFile();
		this.database = TuplUtils.openDatabase(workingFile, configuration);
		this.initializeShutdownHook();
		this.serializationManager = new InMemorySerializationManager();
		this.indexManager = new DocumentBasedIndexManager(this, new TuplIndexManagerBackend(this));
		this.branchManager = new TuplBranchManager(this);
		this.queryManager = new StandardQueryManager(this);
		this.maintenanceManager = new TuplMaintenanceManager(this);
		this.cache = ChronoDBCache.createCacheForConfiguration(configuration);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public IndexManager getIndexManager() {
		return this.indexManager;
	}

	@Override
	public SerializationManager getSerializationManager() {
		return this.serializationManager;
	}

	@Override
	public MaintenanceManager getMaintenanceManager() {
		return this.maintenanceManager;
	}

	@Override
	public BranchManagerInternal getBranchManager() {
		return this.branchManager;
	}

	@Override
	public QueryManager getQueryManager() {
		return this.queryManager;
	}

	@Override
	public ChronoDBCache getCache() {
		return this.cache;
	}

	// =====================================================================================================================
	// INTERNAL METHODS
	// =====================================================================================================================

	@Override
	protected void updateBuildVersionInDatabase() {
		try (DefaultTuplTransaction tx = this.openTransaction()) {
			byte[] buildVersion = tx.load(MANAGEMENT_INDEX_NAME, MANAGEMENT_INDEX__CHRONOS_BUILD_VERSION);
			String versionString = TuplUtils.decodeString(buildVersion);
			if (versionString == null) {
				// no version has been written yet; most likely the database is empty. Write our
				// current version to it
				buildVersion = TuplUtils.encodeString(ChronosVersion.getCurrentVersion().toString());
				tx.store(MANAGEMENT_INDEX_NAME, MANAGEMENT_INDEX__CHRONOS_BUILD_VERSION, buildVersion);
			} else {
				// check the version
				ChronosVersion dbVersion = ChronosVersion.parse(versionString);
				if (dbVersion.compareTo(ChronosVersion.getCurrentVersion()) > 0) {
					// the database has been written by a NEWER version of chronos; we must not touch it!
					throw new ChronosBuildVersionConflictException("The database was written by Chronos '"
							+ dbVersion.toString() + "', but this is the older version '"
							+ ChronosVersion.getCurrentVersion().toString()
							+ "'! Older versions of Chronos cannot open databases created by newer versions!");
				} else {
					// database was created by an older version of chronos; upate it
					buildVersion = TuplUtils.encodeString(ChronosVersion.getCurrentVersion().toString());
					tx.store(MANAGEMENT_INDEX_NAME, MANAGEMENT_INDEX__CHRONOS_BUILD_VERSION, buildVersion);
				}
			}
			tx.commit();
		}
	}

	protected DefaultTuplTransaction openTransaction() {
		return new DefaultTuplTransaction(this.database, this.database.newTransaction());
	}

	protected DefaultTuplTransaction openBogusTransaction() {
		return new DefaultTuplTransaction(this.database, Transaction.BOGUS);
	}

	protected String getDirectory() {
		return this.getConfiguration().getWorkingDirectory().getAbsolutePath();
	}

	/**
	 * Initializes the shutdown hook required to close the internal connection to the MapDB backend.
	 */
	private void initializeShutdownHook() {
		this.addShutdownHook(() -> {
			try {
				this.database.close();
			} catch (Exception e) {
				throw new ChronosIOException("Failed to shut down database! See root cause for details.");
			}
		});
	}

}
