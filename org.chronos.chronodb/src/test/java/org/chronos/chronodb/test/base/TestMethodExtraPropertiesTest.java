package org.chronos.chronodb.test.base;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TestMethodExtraPropertiesTest extends AllChronoDBBackendsTest {

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY, value = "OVERWRITE_WITH_SOURCE")
	public void addingExtraPropertiesWorks() {
		ChronoDB db = this.getChronoDB();
		ChronoDBInternal dbInternal = (ChronoDBInternal) db;
		ChronoDBConfiguration configuration = dbInternal.getConfiguration();
		assertThat(configuration.getConflictResolutionStrategy(), is(ConflictResolutionStrategy.OVERWRITE_WITH_SOURCE));
	}

}
