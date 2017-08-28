package org.chronos.chronodb.test.migration;

import static org.junit.Assert.*;

import java.util.List;

import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.migration.ChronosMigration;
import org.chronos.chronodb.internal.api.migration.MigrationChain;
import org.chronos.chronodb.test.migration.chainA.MigrationA1;
import org.chronos.chronodb.test.migration.chainA.MigrationA2;
import org.chronos.chronodb.test.migration.chainA.MigrationA3;
import org.chronos.common.test.ChronosUnitTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.chronos.common.version.ChronosVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(IntegrationTest.class)
public class MigrationChainTest extends ChronosUnitTest {

	@Test
	public void canScanForMigrationChain() {
		MigrationChain<ChronoDBInternal> chain = MigrationChain.fromPackage("org.chronos.chronodb.test.migration.chainA");
		List<Class<? extends ChronosMigration<ChronoDBInternal>>> actualClasses = chain.getMigrationClasses();
		List<Class<? extends ChronosMigration<ChronoDBInternal>>> expectedClasses = Lists.newArrayList();
		expectedClasses.add(MigrationA1.class);
		expectedClasses.add(MigrationA2.class);
		expectedClasses.add(MigrationA3.class);
		assertEquals(expectedClasses, actualClasses);
	}

	@Test
	public void canLimitMigrationChainArbitrarilyBetweenVersions() {
		MigrationChain<ChronoDBInternal> chain = MigrationChain.fromPackage("org.chronos.chronodb.test.migration.chainA");
		chain = chain.startingAt(ChronosVersion.parse("0.6.0"));
		List<Class<? extends ChronosMigration<ChronoDBInternal>>> actualClasses = chain.getMigrationClasses();
		List<Class<? extends ChronosMigration<ChronoDBInternal>>> expectedClasses = Lists.newArrayList();
		expectedClasses.add(MigrationA3.class);
		assertEquals(expectedClasses, actualClasses);
	}

	@Test
	public void canLimitMigrationChainPreciselyAtVersion() {
		MigrationChain<ChronoDBInternal> chain = MigrationChain.fromPackage("org.chronos.chronodb.test.migration.chainA");
		chain = chain.startingAt(ChronosVersion.parse("0.5.1"));
		List<Class<? extends ChronosMigration<ChronoDBInternal>>> actualClasses = chain.getMigrationClasses();
		List<Class<? extends ChronosMigration<ChronoDBInternal>>> expectedClasses = Lists.newArrayList();
		expectedClasses.add(MigrationA2.class);
		expectedClasses.add(MigrationA3.class);
		assertEquals(expectedClasses, actualClasses);
	}

	@Test
	public void canDetectOverlappingMigrationChains() {
		try {
			MigrationChain.fromPackage("org.chronos.chronodb.test.migration.chainB");
			fail("Managed to create a valid migration chain with overlapping migration paths!");
		} catch (IllegalStateException expected) {
			// pass
		}
	}

}
