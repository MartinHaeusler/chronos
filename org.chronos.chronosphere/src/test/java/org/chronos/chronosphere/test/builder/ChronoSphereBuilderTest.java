package org.chronos.chronosphere.test.builder;

import static org.junit.Assert.*;

import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.test.base.ChronoSphereUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class ChronoSphereBuilderTest extends ChronoSphereUnitTest {

	@Test
	public void canCreateInMemorySphere() {
		ChronoSphere sphere = ChronoSphere.FACTORY.create().inMemoryRepository().build();
		assertNotNull(sphere);
	}

}
