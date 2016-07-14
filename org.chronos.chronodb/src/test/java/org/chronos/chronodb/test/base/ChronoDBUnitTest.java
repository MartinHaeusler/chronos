package org.chronos.chronodb.test.base;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.common.test.ChronosUnitTest;

public class ChronoDBUnitTest extends ChronosUnitTest {

	protected TemporalKeyValueStore getMasterTkvs(final ChronoDB db) {
		return this.getTkvs(db, ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	protected TemporalKeyValueStore getTkvs(final ChronoDB db, final String branchName) {
		Branch branch = db.getBranchManager().getBranch(branchName);
		return ((BranchInternal) branch).getTemporalKeyValueStore();
	}

}
