package org.chronos.chronodb.test.engine.chunkdb;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.impl.ChronoDBConfigurationImpl;
import org.chronos.chronodb.internal.impl.engines.chunkdb.BranchChunkManager;
import org.chronos.chronodb.internal.impl.engines.chunkdb.GlobalChunkManager;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplUtils;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.configuration.ChronosConfigurationUtil;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.io.Files;

@Category(UnitTest.class)
public class GlobalChunkManagerTest extends ChronoDBUnitTest {

	private GlobalChunkManager manager;

	@Before
	public void before() {
		this.manager = new GlobalChunkManager(this.getTestDirectory(), createConfig());
	}

	@After
	public void after() {
		if (this.manager != null) {
			this.manager.shutdown();
		}
	}

	@Test
	public void canCreateGlobalChunkMananger() {
		assertNotNull(this.manager);
	}

	@Test
	public void createsMasterBranchOnStartup() {
		assertTrue(this.manager.hasChunkManagerForBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER));
	}

	@Test
	public void canCreateBranch() {
		assertNull(this.manager.getChunkManagerForBranch("myBranch"));
		assertFalse(this.manager.hasChunkManagerForBranch("myBranch"));
		BranchChunkManager branchChunkManager = this.manager.getOrCreateChunkManagerForBranch("myBranch");
		assertNotNull(branchChunkManager);
		assertTrue(this.manager.hasChunkManagerForBranch("myBranch"));
		assertEquals(branchChunkManager, this.manager.getChunkManagerForBranch("myBranch"));
	}

	@Test
	public void canDetectBranchOnStartup() {
		assertNull(this.manager.getChunkManagerForBranch("myBranch"));
		assertFalse(this.manager.hasChunkManagerForBranch("myBranch"));
		BranchChunkManager branchChunkManager = this.manager.getOrCreateChunkManagerForBranch("myBranch");
		assertNotNull(branchChunkManager);
		assertEquals(branchChunkManager, this.manager.getChunkManagerForBranch("myBranch"));
		this.manager.shutdown();
		// reopen
		this.manager = new GlobalChunkManager(this.getTestDirectory(), createConfig());
		assertNotNull(this.manager.getChunkManagerForBranch("myBranch"));
		assertTrue(this.manager.hasChunkManagerForBranch("myBranch"));
	}

	@Test
	public void canOpenTransactionAndCommit() {
		this.manager.getOrCreateChunkManagerForBranch("myBranch");
		TuplTransaction tx = this.manager.openTransactionOn("myBranch", 0);
		assertNotNull(tx);
		tx.store("test", "key", TuplUtils.encodeString("value"));
		tx.commit();
		this.manager.shutdown();
		// reopen
		this.manager = new GlobalChunkManager(this.getTestDirectory(), createConfig());
		tx = this.manager.openTransactionOn("myBranch", 0);
		assertEquals("value", TuplUtils.decodeString(tx.load("test", "key")));
	}

	@Test
	public void canOpenTransactionAndRollback() {
		this.manager.getOrCreateChunkManagerForBranch("myBranch");
		TuplTransaction tx = this.manager.openTransactionOn("myBranch", 0);
		assertNotNull(tx);
		tx.store("test", "key", TuplUtils.encodeString("value"));
		tx.rollback();
		tx = this.manager.openTransactionOn("myBranch", 0);
		assertNull(tx.load("test", "key"));
	}

	private static ChronoDBConfiguration createConfig() {
		File tempDir = Files.createTempDir();
		File tempFile = new File(tempDir, "test.chronodb");
		tempFile.deleteOnExit();
		tempDir.deleteOnExit();
		Configuration baseConfig = new BaseConfiguration();
		baseConfig.setProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.CHUNKDB.toString());
		baseConfig.setProperty(ChronoDBConfiguration.WORK_FILE, tempFile);
		ChronoDBConfiguration config = ChronosConfigurationUtil.build(baseConfig, ChronoDBConfigurationImpl.class);
		return config;
	}
}
