package org.chronos.chronodb.test.manual;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitException;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.test.util.KillSwitchCollection;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
import org.chronos.chronodb.test.util.model.payload.NamedPayloadNameIndexer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ManualOutOfMemoryErrorTest {

	private static final String TEST_DIR = System.getProperty("user.home") + "/Desktop/ChronoTest";

	private static final boolean COMMIT_INCREMENTAL = true;

	public static void main(final String[] args) throws Exception {
		doXmxCheck();
		KillSwitchCollection killSwitches = new KillSwitchCollection();
		killSwitches.setOnBeforePrimaryIndexUpdate(goOutOfMemory("On before primary index update"));
		killSwitches.setOnBeforeSecondaryIndexUpdate(goOutOfMemory("On secondary index update"));
		killSwitches.setOnBeforeMetadataUpdate(goOutOfMemory("On before metadata update"));
		killSwitches.setOnBeforeCacheUpdate(goOutOfMemory("On before cache upate"));
		killSwitches.setOnBeforeNowTimestampUpdate(goOutOfMemory("On before now timestamp update"));
		killSwitches.setOnBeforeTransactionCommitted(goOutOfMemory("On before transaction committed"));

		File directory = new File(TEST_DIR);
		if (directory.exists() == false) {
			boolean created = directory.mkdirs();
			if (!created) {
				throw new RuntimeException("Failed to create test directory in '" + directory.getAbsolutePath() + "'!");
			}
			System.out.println("Test directory is located here: '" + directory.getAbsolutePath() + "'");
		}
		FileUtils.cleanDirectory(directory);
		File dbFile = new File(directory, "test.chronodb");
		dbFile.createNewFile();
		ChronoDB db = ChronoDB.FACTORY.create()
				//
				.mapDbDatabase(dbFile)
				//
				.withLruCacheOfSize(1000)
				//
				.withLruQueryCacheOfSize(10)
				//
				.assumeCachedValuesAreImmutable(true)
				//
				.withProperty(ChronoDBConfiguration.DEBUG, "true")
				//
				.build();
		System.out.println("DB created");
		runTest(db, killSwitches, COMMIT_INCREMENTAL);
	}

	private static void runTest(final ChronoDB db, final KillSwitchCollection callbacks,
			final boolean useIncrementalCommits) {
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
			final boolean inc = useIncrementalCommits;
			Thread thread = new Thread(() -> {
				doFaultyCommit(db, inc);
			});
			thread.setUncaughtExceptionHandler((t, ex) -> {
				ex.printStackTrace();
			});
			thread.start();
			try {
				thread.join();
			} catch (InterruptedException e) {
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

	private static void doFaultyCommit(final ChronoDB db, final boolean incrementalCommit) {
		try {
			ChronoDBTransaction txFail = db.tx();
			if (incrementalCommit) {
				txFail.commitIncremental();
			}
			txFail.put("one", NamedPayload.create1KB("Babadu"));
			txFail.put("four", NamedPayload.create1KB("Another"));
			if (incrementalCommit) {
				txFail.commitIncremental();
			}
			txFail.commit();
			fail("Commit was accepted even though kill switch was installed!");
		} catch (ChronoDBCommitException exception) {
			// pass
		}
	}

	private static void doXmxCheck() {
		long maxMemory = Runtime.getRuntime().maxMemory();
		long oneGBinBytes = 1 /* GB */ * 1024L /* MB */ * 1024L /* KB */ * 1024L /* B */;
		if (maxMemory > oneGBinBytes) {
			System.out.println(
					"WARNING: More than 1GB of memory was allocated to this JVM process. Please consider setting -Xmx to a smaller value.");
		}
	}

	private static Consumer<ChronoDBTransaction> goOutOfMemory(final String message) {
		return tx -> {
			if (message != null) {
				System.out.println(message);
			}
			System.out.println("INITIATING OUT-OF-MEMORY PROCEDURE!");
			List<byte[]> data = Lists.newArrayList();
			while (true) {
				data.add(new byte[1_000]);
			}
		};
	}
}
