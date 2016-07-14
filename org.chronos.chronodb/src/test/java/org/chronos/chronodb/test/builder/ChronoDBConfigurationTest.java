package org.chronos.chronodb.test.builder;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.impl.index.querycache.NoIndexQueryCache;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.exceptions.ChronosConfigurationException;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ChronoDBConfigurationTest extends ChronoDBUnitTest {

	@Test
	public void canCreateChronoDbWithBuilder() {
		ChronoDB db = ChronoDB.FACTORY.create().inMemoryDatabase().build();
		assertNotNull(db);
	}

	@Test
	public void canEnableCachingWithInMemoryBuilder() {
		ChronoDB db = ChronoDB.FACTORY.create().inMemoryDatabase().withLruCacheOfSize(1000).build();
		assertNotNull(db);
		try {
			this.assertHasCache(db);
		} finally {
			db.close();
		}
	}

	@Test
	public void canEnableCachingWithMapDbBuilder() {
		File dbFile = new File(this.getTestDirectory(), UUID.randomUUID().toString().replaceAll("-", "") + ".chronodb");
		try {
			dbFile.createNewFile();
		} catch (IOException e) {
			fail(e.toString());
		}
		ChronoDB db = ChronoDB.FACTORY.create().embeddedDatabase(dbFile).withLruCacheOfSize(1000).build();
		assertNotNull(db);
		try {
			this.assertHasCache(db);
		} finally {
			db.close();
		}
	}

	@Test
	public void canEnableCachingWithJdbcBuilder() {
		String name = UUID.randomUUID().toString().replace("-", "");
		ChronoDB db = ChronoDB.FACTORY.create().jdbcDatabase("jdbc:h2:mem:" + name).withLruCacheOfSize(1000).build();
		assertNotNull(db);
		try {
			this.assertHasCache(db);
		} finally {
			db.close();
		}
	}

	@Test
	public void canEnableCachingViaPropertiesFile() {
		File propertiesFile = this.getSrcTestResourcesFile("chronoCacheConfigTest_correct.properties");
		ChronoDB chronoDB = ChronoDB.FACTORY.create().fromPropertiesFile(propertiesFile).build();
		assertNotNull(chronoDB);
		try {
			this.assertHasCache(chronoDB);
		} finally {
			chronoDB.close();
		}
	}

	@Test
	public void cacheMaxSizeSettingIsRequiredIfCachingIsEnabled() {
		File propertiesFile = this.getSrcTestResourcesFile("chronoCacheConfigTest_wrong.properties");
		try {
			ChronoDB.FACTORY.create().fromPropertiesFile(propertiesFile).build();
			fail("Managed to create cached ChronoDB instance without specifying the Max Cache Size!");
		} catch (ChronosConfigurationException expected) {
			// pass
		}
	}

	private void assertHasCache(final ChronoDB db) {
		TemporalKeyValueStore store = ((BranchInternal) db.getBranchManager().getMasterBranch())
				.getTemporalKeyValueStore();
		// make sure that we have a cache
		assertNotNull(store.getCache());
		// make sure it's not an instance of our "fake" cache (which is essentially a no-cache)
		assertFalse(store.getCache() instanceof NoIndexQueryCache);
	}
}
