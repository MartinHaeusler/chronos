package org.chronos.chronodb.test.base;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.common.logging.ChronoLogger;
import org.junit.After;
import org.junit.Assume;

public abstract class AllChronoDBBackendsTest extends AllBackendsTest {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private ChronoDB db;

	// =================================================================================================================
	// GETTERS & SETTERS
	// =================================================================================================================

	protected ChronoDB getChronoDB() {
		if (this.db == null) {
			this.db = this.instantiateChronoDB(this.backend);
		}
		return this.db;
	}

	// =================================================================================================================
	// JUNIT CONTROL
	// =================================================================================================================

	@After
	public void cleanUp() {
		ChronoLogger.logDebug("Closing ChronoDB on backend '" + this.backend + "'.");
		this.getChronoDB().close();
	}

	// =================================================================================================================
	// UTILITY
	// =================================================================================================================

	protected ChronoDB reinstantiateDB() {
		ChronoLogger.logDebug("Reinstantiating ChronoDB on backend '" + this.backend + "'.");
		this.db.close();
		this.db = this.instantiateChronoDB(this.backend);
		return this.db;
	}

	protected ChronoDB closeAndReopenDB() {
		ChronoDB db = this.getChronoDB();
		// this won't work for in-memory (obviously)
		Assume.assumeFalse(db.getConfiguration().getBackendType().equals(ChronosBackend.INMEMORY));
		ChronoDBConfiguration configuration = db.getConfiguration();
		db.close();
		this.db = ChronoDB.FACTORY.create().fromConfiguration(configuration.asCommonsConfiguration()).build();
		return this.db;
	}

	protected TemporalKeyValueStore getMasterTkvs() {
		return this.getTkvs(this.getChronoDB(), ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	protected TemporalKeyValueStore getMasterTkvs(final ChronoDB db) {
		return this.getTkvs(db, ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	protected TemporalKeyValueStore getTkvs(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		return this.getTkvs(this.getChronoDB(), branchName);
	}

	protected TemporalKeyValueStore getTkvs(final ChronoDB db, final String branchName) {
		Branch branch = db.getBranchManager().getBranch(branchName);
		return ((BranchInternal) branch).getTemporalKeyValueStore();
	}

}
