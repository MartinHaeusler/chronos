package org.chronos.chronodb.internal.impl.engines.mapdb;

import java.io.File;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.MaintenanceManager;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.api.exceptions.ChronosBuildVersionConflictException;
import org.chronos.chronodb.internal.api.BranchManagerInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.api.query.QueryManager;
import org.chronos.chronodb.internal.impl.engines.base.AbstractChronoDB;
import org.chronos.chronodb.internal.impl.engines.inmemory.InMemorySerializationManager;
import org.chronos.chronodb.internal.impl.index.DocumentBasedIndexManager;
import org.chronos.chronodb.internal.impl.mapdb.MapDBTransaction;
import org.chronos.chronodb.internal.impl.query.StandardQueryManager;
import org.chronos.common.version.ChronosVersion;
import org.mapdb.Atomic.Var;
import org.mapdb.DB;
import org.mapdb.DBMaker;

public class MapDBChronoDB extends AbstractChronoDB {

	// =====================================================================================================================
	// STATIC FIELDS
	// =====================================================================================================================

	public static final String VARIABLE_NAME__CHRONOS_BUILD_VERSION = "chronos.buildVersion";

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final DB db;

	private final BranchManagerInternal branchManager;
	private final SerializationManager serializationManager;
	private final IndexManager indexManager;
	private final QueryManager queryManager;
	private final MaintenanceManager maintenanceManager;

	private final ChronoDBCache cache;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public MapDBChronoDB(final ChronoDBConfiguration configuration) {
		super(configuration);
		this.db = DBMaker.fileDB(configuration.getWorkingFile()).make();
		// remember the directory we are working in
		this.branchManager = new MapDBBranchManager(this);
		// we use the in-memory solution here; when we allow serialization
		// customization and configuration, we must use a solution that persists
		// the configured version in the database.
		this.serializationManager = new InMemorySerializationManager();
		this.indexManager = new DocumentBasedIndexManager(this, new MapDBIndexManagerBackend(this));
		this.queryManager = new StandardQueryManager(this);
		this.maintenanceManager = new MapDBMaintenanceManager(this);
		this.cache = ChronoDBCache.createCacheForConfiguration(configuration);
		this.initializeShutdownHook();
		// perform the initial commit (primarily contains setup of empty B-Trees)
		this.db.commit();
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

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	/**
	 * Opens a transaction on the underlying MapDB instance.
	 *
	 * <p>
	 * It is <b>required</b> to call {@link MapDBTransaction#close()} when the transaction is not needed anymore. It is
	 * recommended to do so using try-with-resources blocks, as {@link MapDBTransaction} implements
	 * {@link AutoCloseable}:
	 *
	 * <pre>
	 * try (MapDBTransaction tx = chronoDB.openTransaction()) {
	 * 	// do work with tx
	 * }
	 * </pre>
	 *
	 * @return The MapDB transaction. Must be closed explicitly. Never <code>null</code>.
	 */
	protected MapDBTransaction openTransaction() {
		try {
			return new MapDBTransaction(this.db);
		} catch (Exception e) {
			throw new ChronoDBStorageBackendException("Could not open a transaction on the backing MapDB!");
		}
	}

	/**
	 * Returns the directory in which this instance of {@link ChronoDB} is working.
	 *
	 * @return The directory. Never <code>null</code>. Guaranteed to be a directory.
	 */
	protected File getDirectory() {
		return this.getConfiguration().getWorkingDirectory();
	}

	@Override
	protected void updateBuildVersionInDatabase() {
		try (MapDBTransaction tx = this.openTransaction()) {
			Var<String> variable = tx.atomicVar(VARIABLE_NAME__CHRONOS_BUILD_VERSION);
			String versionString = variable.get();
			if (versionString == null) {
				// no version has been written yet; most likely the database is empty. Write our
				// current version to it
				variable.set(ChronosVersion.getCurrentVersion().toString());
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
					variable.set(ChronosVersion.getCurrentVersion().toString());
				}
			}
			tx.commit();
		}
	}

	/**
	 * Initializes the shutdown hook required to close the internal connection to the MapDB backend.
	 */
	private void initializeShutdownHook() {
		this.addShutdownHook(() -> {
			// TODO SAFETY MAPDB: this commit shouldn't be necessary. See comment below.
			// Some JUnit tests have revealed that operations on a MapDBChronoDB can cause modifications
			// to the underlying MapDB instance which are unintentional and therefore not commited during
			// normal operation. When closing, MapDB issues an warning as a result. Committing all changes
			// before closing the DB fixes the issue, but this might be only a "symptomatic treatment", and
			// requires closer investigation. The following tests produce the warning:
			// - IndexingTest#readingNonExistentIndexFailsGracefully[Using FILE]
			// - KeyspaceTest#creatingAndReadingAKeyspaceWorks[Using FILE]
			// - RangedGetTest#rangedGetBehavesCorrectlyOnNonExistingKeyspaces[Using FILE]
			this.db.commit();
			this.db.close();
		});
	}

}
