package org.chronos.chronodb.internal.impl.engines.inmemory;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.MaintenanceManager;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.internal.api.BranchManagerInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.api.query.QueryManager;
import org.chronos.chronodb.internal.impl.engines.base.AbstractChronoDB;
import org.chronos.chronodb.internal.impl.index.DocumentBasedIndexManager;
import org.chronos.chronodb.internal.impl.query.StandardQueryManager;
import org.chronos.common.version.ChronosVersion;

public class InMemoryChronoDB extends AbstractChronoDB {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private InMemoryBranchManager branchManager;
	private InMemorySerializationManager serializationManager;
	private IndexManager indexManager;
	private StandardQueryManager queryManager;
	private InMemoryMaintenanceManager maintenanceManager;

	private ChronoDBCache cache;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public InMemoryChronoDB(final ChronoDBConfiguration configuration) {
		super(configuration);
		this.setupManagers();
		this.cache = ChronoDBCache.createCacheForConfiguration(configuration);
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
	public QueryManager getQueryManager() {
		return this.queryManager;
	}

	@Override
	public IndexManager getIndexManager() {
		return this.indexManager;
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
		// we can safely ignore this; in-memory DBs never need to be migrated.
	}

	// =====================================================================================================================
	// INTERNAL METHODS
	// =====================================================================================================================

	@Override
	protected void updateBuildVersionInDatabase() {
		// this isn't applicable for in-memory databases because the chronos build version
		// can never change during a live JVM session.
	}

	private void setupManagers() {
		this.branchManager = new InMemoryBranchManager(this);
		this.serializationManager = new InMemorySerializationManager();
		this.queryManager = new StandardQueryManager(this);
		this.indexManager = new DocumentBasedIndexManager(this, new InMemoryIndexManagerBackend(this));
		this.maintenanceManager = new InMemoryMaintenanceManager(this);
	}

}
