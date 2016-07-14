package org.chronos.chronodb.test.builder;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ChronoDBCreationTest extends AllChronoDBBackendsTest {

	@Test
	public void dbCreationWorks() {
		ChronoDB chronoDB = this.getChronoDB();
		assertNotNull("Failed to instantiate ChronoDB on Backend '" + this.getChronoBackendName() + "'!", chronoDB);
	}

}
