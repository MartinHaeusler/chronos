package org.chronos.chronodb.test.engine.safetyfeatures;

import static org.junit.Assert.*;

import java.util.function.Consumer;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitException;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronodb.test.util.KillSwitchCollection;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
import org.chronos.chronodb.test.util.model.payload.NamedPayloadNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class RollbackToWALTest extends AllChronoDBBackendsTest {

	// =================================================================================================================
	// REGULAR COMMIT TESTS
	// =================================================================================================================

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingTxTest_BeforePrimaryIndexUpdate_WithException() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforePrimaryIndexUpdate(crashWith(new RuntimeException()));
		this.runTest(killSwitches, ThreadMode.SINGLE, CommitMode.REGULAR);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingTxTest_BeforePrimaryIndexUpdate_WithThreadKill() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforePrimaryIndexUpdate(killThread());
		this.runTest(killSwitches, ThreadMode.THREAD_PER_COMMIT, CommitMode.REGULAR);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingTxTest_BeforeSecondaryIndexUpdate_WithException() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeSecondaryIndexUpdate(crashWith(new RuntimeException()));
		this.runTest(killSwitches, ThreadMode.SINGLE, CommitMode.REGULAR);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingTxTest_BeforeSecondaryIndexUpdate_WithThreadKill() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeSecondaryIndexUpdate(killThread());
		this.runTest(killSwitches, ThreadMode.THREAD_PER_COMMIT, CommitMode.REGULAR);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingTxTest_BeforeMetadataUpdate_WithException() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeMetadataUpdate(crashWith(new RuntimeException()));
		this.runTest(killSwitches, ThreadMode.SINGLE, CommitMode.REGULAR);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingTxTest_BeforeMetadataUpdate_WithThreadKill() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeMetadataUpdate(killThread());
		this.runTest(killSwitches, ThreadMode.THREAD_PER_COMMIT, CommitMode.REGULAR);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingTxTest_BeforeCacheUpdate_WithException() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeCacheUpdate(crashWith(new RuntimeException()));
		this.runTest(killSwitches, ThreadMode.SINGLE, CommitMode.REGULAR);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingTxTest_BeforeCacheUpdate_WithThreadKill() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeCacheUpdate(killThread());
		this.runTest(killSwitches, ThreadMode.THREAD_PER_COMMIT, CommitMode.REGULAR);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingTxTest_BeforeNowTimestampUpdate_WithException() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeNowTimestampUpdate(crashWith(new RuntimeException()));
		this.runTest(killSwitches, ThreadMode.SINGLE, CommitMode.REGULAR);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingTxTest_BeforeNowTimestampUpdate_WithThreadKill() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeNowTimestampUpdate(killThread());
		this.runTest(killSwitches, ThreadMode.THREAD_PER_COMMIT, CommitMode.REGULAR);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingTxTest_BeforeTransactionCommittedUpdate_WithException() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeTransactionCommitted(crashWith(new RuntimeException()));
		this.runTest(killSwitches, ThreadMode.SINGLE, CommitMode.REGULAR);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingTxTest_BeforeTransactionCommitted_WithThreadKill() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeTransactionCommitted(killThread());
		this.runTest(killSwitches, ThreadMode.THREAD_PER_COMMIT, CommitMode.REGULAR);
	}

	// =================================================================================================================
	// INCREMENTAL COMMIT TESTS
	// =================================================================================================================

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingIncTxTest_BeforePrimaryIndexUpdate_WithException() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforePrimaryIndexUpdate(crashWith(new RuntimeException()));
		this.runTest(killSwitches, ThreadMode.SINGLE, CommitMode.INCREMENTAL);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingIncTxTest_BeforePrimaryIndexUpdate_WithThreadKill() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforePrimaryIndexUpdate(killThread());
		this.runTest(killSwitches, ThreadMode.THREAD_PER_COMMIT, CommitMode.INCREMENTAL);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingIncTxTest_BeforeSecondaryIndexUpdate_WithException() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeSecondaryIndexUpdate(crashWith(new RuntimeException()));
		this.runTest(killSwitches, ThreadMode.SINGLE, CommitMode.INCREMENTAL);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingIncTxTest_BeforeSecondaryIndexUpdate_WithThreadKill() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeSecondaryIndexUpdate(killThread());
		this.runTest(killSwitches, ThreadMode.THREAD_PER_COMMIT, CommitMode.INCREMENTAL);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingIncTxTest_BeforeMetadataUpdate_WithException() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeMetadataUpdate(crashWith(new RuntimeException()));
		this.runTest(killSwitches, ThreadMode.SINGLE, CommitMode.INCREMENTAL);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingIncTxTest_BeforeMetadataUpdate_WithThreadKill() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeMetadataUpdate(killThread());
		this.runTest(killSwitches, ThreadMode.THREAD_PER_COMMIT, CommitMode.INCREMENTAL);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingIncTxTest_BeforeCacheUpdate_WithException() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeCacheUpdate(crashWith(new RuntimeException()));
		this.runTest(killSwitches, ThreadMode.SINGLE, CommitMode.INCREMENTAL);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingIncTxTest_BeforeCacheUpdate_WithThreadKill() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeCacheUpdate(killThread());
		this.runTest(killSwitches, ThreadMode.THREAD_PER_COMMIT, CommitMode.INCREMENTAL);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingIncTxTest_BeforeNowTimestampUpdate_WithException() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeNowTimestampUpdate(crashWith(new RuntimeException()));
		this.runTest(killSwitches, ThreadMode.SINGLE, CommitMode.INCREMENTAL);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingIncTxTest_BeforeNowTimestampUpdate_WithThreadKill() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeNowTimestampUpdate(killThread());
		this.runTest(killSwitches, ThreadMode.THREAD_PER_COMMIT, CommitMode.INCREMENTAL);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingIncTxTest_BeforeTransactionCommittedUpdate_WithException() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeTransactionCommitted(crashWith(new RuntimeException()));
		this.runTest(killSwitches, ThreadMode.SINGLE, CommitMode.INCREMENTAL);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
	public void crashingIncTxTest_BeforeTransactionCommitted_WithThreadKill() {
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforeTransactionCommitted(killThread());
		this.runTest(killSwitches, ThreadMode.THREAD_PER_COMMIT, CommitMode.INCREMENTAL);
	}

	// =================================================================================================================
	// ACTUAL TEST METHOD
	// =================================================================================================================

	private void runTest(final KillSwitchCollection callbacks, final ThreadMode threadMode, final CommitMode commitMode) {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("name", new NamedPayloadNameIndexer());

		{
			ChronoDBTransaction tx = db.tx();
			tx.put("one", NamedPayload.create1KB("Hello World"));
			tx.put("two", NamedPayload.create1KB("Foo Bar"));
			tx.put("three", NamedPayload.create1KB("Baz"));
			tx.commit();
		}

		// check that everything is in the database
		assertEquals(Sets.newHashSet("one", "two", "three"), db.tx().keySet());
		// check that the secondary index is okay
		assertEquals(2, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("ba").count());

		long nowAfterCommit = db.getBranchManager().getMasterBranch().getNow();

		Branch branch = db.getBranchManager().getMasterBranch();
		TemporalKeyValueStore tkvs = ((BranchInternal) branch).getTemporalKeyValueStore();
		// set up the callbacks
		tkvs.setDebugCallbackBeforePrimaryIndexUpdate(callbacks.getOnBeforePrimaryIndexUpdate());
		tkvs.setDebugCallbackBeforeSecondaryIndexUpdate(callbacks.getOnBeforeSecondaryIndexUpdate());
		tkvs.setDebugCallbackBeforeMetadataUpdate(callbacks.getOnBeforeMetadataUpdate());
		tkvs.setDebugCallbackBeforeCacheUpdate(callbacks.getOnBeforeCacheUpdate());
		tkvs.setDebugCallbackBeforeNowTimestampUpdate(callbacks.getOnBeforeNowTimestampUpdate());
		tkvs.setDebugCallbackBeforeTransactionCommitted(callbacks.getOnBeforeTransactionCommitted());
		{
			if (ThreadMode.THREAD_PER_COMMIT.equals(threadMode)) {
				Thread thread = new Thread(() -> {
					this.doFaultyCommit(db, commitMode);
				});
				thread.start();
				try {
					thread.join();
				} catch (InterruptedException e) {
				}
			} else {
				this.doFaultyCommit(db, commitMode);
			}
		}
		// uninstall the kill switch
		tkvs.setDebugCallbackBeforePrimaryIndexUpdate(null);
		tkvs.setDebugCallbackBeforeSecondaryIndexUpdate(null);
		tkvs.setDebugCallbackBeforeMetadataUpdate(null);
		tkvs.setDebugCallbackBeforeCacheUpdate(null);
		tkvs.setDebugCallbackBeforeNowTimestampUpdate(null);
		tkvs.setDebugCallbackBeforeTransactionCommitted(null);

		// make sure that none of the data managed to get through
		assertEquals(nowAfterCommit, db.getBranchManager().getMasterBranch().getNow());
		// check that everything is in the database
		assertEquals(Sets.newHashSet("one", "two", "three"), db.tx().keySet());
		// check that the secondary index is okay
		assertEquals(2, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("ba").count());

		// make another regular commit
		{
			ChronoDBTransaction tx = db.tx();
			tx.put("one", NamedPayload.create1KB("42"));
			tx.put("two", NamedPayload.create1KB("Foo")); // remove the "Bar"
			tx.put("five", NamedPayload.create1KB("High Five"));
			tx.commit();
		}
		// make sure that the changes of the last commit are present, but the ones from the failed commit are not
		assertEquals(Sets.newHashSet("one", "two", "three", "five"), db.tx().keySet());
		// check that the secondary index is okay
		assertEquals(1, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("ba").count());
		assertEquals(0, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("another").count());
		assertEquals(1, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("five").count());
	}

	private void doFaultyCommit(final ChronoDB db, final CommitMode commitMode) {
		try {
			ChronoDBTransaction txFail = db.tx();
			if (CommitMode.INCREMENTAL.equals(commitMode)) {
				txFail.commitIncremental();
			}
			txFail.put("one", NamedPayload.create1KB("Babadu"));
			txFail.put("four", NamedPayload.create1KB("Another"));
			if (CommitMode.INCREMENTAL.equals(commitMode)) {
				txFail.commitIncremental();
			}
			txFail.commit();
			fail("Commit was accepted even though kill switch was installed!");
		} catch (ChronoDBCommitException exception) {
			// pass
		}
	}

	// =================================================================================================================
	// HELPER METHODS
	// =================================================================================================================

	private static Consumer<ChronoDBTransaction> crashWith(final Throwable t) {
		return tx -> {
			sneakyThrow(t);
		};
	}

	@SuppressWarnings("deprecation")
	private static Consumer<ChronoDBTransaction> killThread() {
		return tx -> {
			Thread.currentThread().stop();
		};
	}

	@SuppressWarnings("unchecked")
	private static <T extends Throwable> void sneakyThrow(final Throwable t) throws T {
		throw (T) t;
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private static enum ThreadMode {
		SINGLE, THREAD_PER_COMMIT
	}

	private static enum CommitMode {
		REGULAR, INCREMENTAL
	}

}
