package org.chronos.chronodb.internal.impl.engines.chunkdb;

import java.io.File;

import org.chronos.chronodb.api.MaintenanceManager;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.api.exceptions.ChronosBuildVersionConflictException;
import org.chronos.chronodb.internal.api.BranchManagerInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.api.query.QueryManager;
import org.chronos.chronodb.internal.impl.engines.base.AbstractChronoDB;
import org.chronos.chronodb.internal.impl.engines.chunkdb.index.ChunkDbIndexManager;
import org.chronos.chronodb.internal.impl.engines.inmemory.InMemorySerializationManager;
import org.chronos.chronodb.internal.impl.engines.tupl.DefaultTuplTransaction;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplUtils;
import org.chronos.chronodb.internal.impl.query.StandardQueryManager;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.version.ChronosVersion;
import org.cojen.tupl.Database;

public class ChunkedChronoDB extends AbstractChronoDB {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	protected static final String FILENAME__BRANCHES_DIRECTORY = "branches";

	// the names of various indices and variables used in the central chronos file.
	protected static final String INDEXNAME__CHRONOS_MANAGEMENT = "chronos.management";
	protected static final String CHRONOS_MANAGEMENT__CHRONOS_VERSION = "chronos.version";

	protected static final String INDEXNAME__BRANCH_TO_NOW = "branch_to_now";
	protected static final String INDEXNAME__BRANCH_TO_WAL = "branch_to_wal";

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final File branchesDirectory;

	private final GlobalChunkManager chunkManager;
	private final QueryManager queryManager;
	private final BranchManagerInternal branchManager;
	private final SerializationManager serializationManager;
	private final ChunkDbIndexManager indexManager;
	private final MaintenanceManager maintenanceManager;

	private final ChronoDBCache cache;

	private final Database rootDB;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public ChunkedChronoDB(final ChronoDBConfiguration configuration) {
		super(configuration);
		this.branchesDirectory = new File(configuration.getWorkingDirectory(), FILENAME__BRANCHES_DIRECTORY);
		if (this.branchesDirectory.exists() == false) {
			if (!this.branchesDirectory.mkdirs()) {
				throw new IllegalStateException(
						"Failed to create directory '" + this.branchesDirectory.getAbsolutePath() + "'!");
			}
		}
		this.rootDB = TuplUtils.openDatabase(configuration.getWorkingFile(), configuration);
		this.serializationManager = new InMemorySerializationManager();
		this.chunkManager = new GlobalChunkManager(this.branchesDirectory, configuration);
		this.queryManager = new StandardQueryManager(this);
		this.indexManager = new ChunkDbIndexManager(this);
		this.branchManager = new ChunkDbBranchManager(this);
		this.maintenanceManager = new ChunkDbMaintenanceManager(this);
		this.cache = ChronoDBCache.createCacheForConfiguration(configuration);
		this.initializeShutdownHook();
	}

	// =================================================================================================================
	// ABSTRACT METHOD IMPLEMENTATIONS
	// =================================================================================================================

	@Override
	public ChunkDbIndexManager getIndexManager() {
		return this.indexManager;
	}

	@Override
	public SerializationManager getSerializationManager() {
		return this.serializationManager;
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
	public MaintenanceManager getMaintenanceManager() {
		return this.maintenanceManager;
	}

	@Override
	public ChronoDBCache getCache() {
		return this.cache;
	}

	@Override
	protected void updateBuildVersionInDatabase() {
		try (TuplTransaction tx = this.openTx()) {
			byte[] versionData = tx.load(INDEXNAME__CHRONOS_MANAGEMENT, CHRONOS_MANAGEMENT__CHRONOS_VERSION);
			String versionString = TuplUtils.decodeString(versionData);
			if (versionString == null) {
				// no version has been written yet; most likely the database is empty. Write our
				// current version to it
				String currentVersion = ChronosVersion.getCurrentVersion().toString();
				byte[] currentVersionData = TuplUtils.encodeString(currentVersion);
				tx.store(INDEXNAME__CHRONOS_MANAGEMENT, CHRONOS_MANAGEMENT__CHRONOS_VERSION, currentVersionData);
			} else {
				// check the version
				ChronosVersion dbVersion = ChronosVersion.parse(versionString);
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
					// database was created by an older version of chronos; update it
					byte[] currentVersionData = TuplUtils.encodeString(currentVersion.toString());
					tx.store(INDEXNAME__CHRONOS_MANAGEMENT, CHRONOS_MANAGEMENT__CHRONOS_VERSION, currentVersionData);
				}
			}
			tx.commit();
		}
	}

	// =================================================================================================================
	// ADDITIONAL API
	// =================================================================================================================

	public GlobalChunkManager getChunkManager() {
		return this.chunkManager;
	}

	public TuplTransaction openTx() {
		return new DefaultTuplTransaction(this.rootDB, this.rootDB.newTransaction());
	}

	// =================================================================================================================
	// HELPER METHODS
	// =================================================================================================================

	private void initializeShutdownHook() {
		this.addShutdownHook(() -> {
			this.chunkManager.shutdown();
			TuplUtils.shutdownQuietly(this.rootDB);
		});
	}
}
