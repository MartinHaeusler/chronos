package org.chronos.chronosphere.test.configuration;

import static org.junit.Assert.*;

import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.internal.api.ChronoSphereInternal;
import org.chronos.chronosphere.test.base.AllChronoSphereBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ChronoSphereConfigurationTest extends AllChronoSphereBackendsTest {

	@Test
	public void configurationIsPresent() {
		ChronoSphereInternal repository = this.getChronoSphere();
		assertNotNull(repository);
		assertNotNull(repository.getConfiguration());
		assertNotNull(repository.getRootGraph());
	}

	@Test
	public void canOpenAndCloseRepository() {
		ChronoSphere repository = this.getChronoSphere();
		// assert that it's open
		assertTrue(repository.isOpen());
		assertFalse(repository.isClosed());
		// close it now
		repository.close();
		// assert that it's closed
		assertFalse(repository.isOpen());
		assertTrue(repository.isClosed());
	}

}
