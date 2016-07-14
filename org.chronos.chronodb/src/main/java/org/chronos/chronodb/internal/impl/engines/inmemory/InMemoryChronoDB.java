package org.chronos.chronodb.internal.impl.engines.inmemory;

import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.internal.api.BranchManagerInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.query.QueryManager;
import org.chronos.chronodb.internal.impl.engines.base.AbstractChronoDB;
import org.chronos.chronodb.internal.impl.index.StandardIndexManager;
import org.chronos.chronodb.internal.impl.query.StandardQueryManager;

public class InMemoryChronoDB extends AbstractChronoDB {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private InMemoryBranchManager branchManager;
	private InMemorySerializationManager serializationManager;
	private IndexManager indexManager;
	private StandardQueryManager queryManager;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public InMemoryChronoDB(final ChronoDBConfiguration configuration) {
		super(configuration);
		this.setupManagers();
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
		this.indexManager = new StandardIndexManager(this, new InMemoryIndexManagerBackend(this));
	}

}
