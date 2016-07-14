package org.chronos.chronodb.test.settings;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ChronoDBConfigurationTest extends AllChronoDBBackendsTest {

	@Test
	public void debugModeIsAlwaysEnabledInTests() {
		ChronoDB db = this.getChronoDB();
		assertTrue(db.getConfiguration().isDebugModeEnabled());
	}
}
