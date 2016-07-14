package org.chronos.chronodb.test.base;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TestMethodExtraPropertiesTest extends AllChronoDBBackendsTest {

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.ENABLE_BLIND_OVERWRITE_PROTECTION, value = "false")
	public void addingExtraPropertiesWorks() {
		ChronoDB db = this.getChronoDB();
		ChronoDBInternal dbInternal = (ChronoDBInternal) db;
		ChronoDBConfiguration configuration = dbInternal.getConfiguration();
		assertFalse(configuration.isBlindOverwriteProtectionEnabled());
	}

}
