package org.chronos.chronodb.test.manual;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.test.util.KillSwitchCollection;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
import org.chronos.chronodb.test.util.model.payload.NamedPayloadNameIndexer;

import com.google.common.collect.Sets;

public class ManualProcessKillTest {

	private static final String TEST_DIR = System.getProperty("user.home") + "/Desktop/ChronoTest";

	private static final boolean COMMIT_INCREMENTAL = true;

	public static void main(final String[] args) throws Exception {
		File directory = new File(TEST_DIR);
		if (directory.exists() == false) {
			boolean created = directory.mkdirs();
			if (!created) {
				throw new RuntimeException("Failed to create test directory in '" + directory.getAbsolutePath() + "'!");
			}
			System.out.println("Test directory is located here: '" + directory.getAbsolutePath() + "'");
		}
		boolean testDirEmpty = false;
		if (directory.list().length > 0) {
			System.out.println("Found data in directory. Performing after-commit check...");
			testDirEmpty = false;
		} else {
			System.out.println("Test directory is empty. Performing commits...");
			testDirEmpty = true;
		}
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
		if (testDirEmpty) {
			KillSwitchCollection killSwitches = new KillSwitchCollection();
			killSwitches.setOnBeforePrimaryIndexUpdate(waitForSeconds(3, "Before Primary Index Update"));
			killSwitches.setOnBeforeSecondaryIndexUpdate(waitForSeconds(3, "Before Secondary Index Update"));
			killSwitches.setOnBeforeMetadataUpdate(waitForSeconds(3, "Before Metadata Update"));
			killSwitches.setOnBeforeCacheUpdate(waitForSeconds(3, "Before Cache Update"));
			killSwitches.setOnBeforeNowTimestampUpdate(waitForSeconds(3, "Before Now Timestamp Update"));
			killSwitches.setOnBeforeTransactionCommitted(waitForSeconds(3, "Before Transaction Committed"));
			System.out.println(
					"Performing insertions by using " + (COMMIT_INCREMENTAL ? "INCREMENTAL" : "REGULAR") + " commits.");
			performInsertions(db, killSwitches, COMMIT_INCREMENTAL);
		} else {
			performAfterCommitChecks(db);
		}

	}

	private static void performInsertions(final ChronoDB db, final KillSwitchCollection callbacks,
			final boolean commitIncremental) {
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

			ChronoDBTransaction txFail = db.tx();
			if (commitIncremental) {
				txFail.commitIncremental();
			}
			txFail.put("one", NamedPayload.create1KB("Babadu"));
			txFail.put("four", NamedPayload.create1KB("Another"));
			if (commitIncremental) {
				txFail.commitIncremental();
			}
			txFail.commit();
		}
		System.out.println("You failed to kill the JVM process! Clearing test directory...");
		db.close();
		File directory = new File(TEST_DIR);
		try {
			FileUtils.cleanDirectory(directory);
			System.out.println("Cleaned test directory");
		} catch (IOException e) {
			e.printStackTrace();
		}

		// uninstall the kill switch
		tkvs.setDebugCallbackBeforePrimaryIndexUpdate(null);
		tkvs.setDebugCallbackBeforeSecondaryIndexUpdate(null);
		tkvs.setDebugCallbackBeforeMetadataUpdate(null);
		tkvs.setDebugCallbackBeforeCacheUpdate(null);
		tkvs.setDebugCallbackBeforeNowTimestampUpdate(null);
		tkvs.setDebugCallbackBeforeTransactionCommitted(null);
	}

	private static void performAfterCommitChecks(final ChronoDB db) {
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
		System.out.println("After commit checks succeeded. Cleaining test directory...");
		db.close();
		File directory = new File(TEST_DIR);
		try {
			FileUtils.cleanDirectory(directory);
			System.out.println("Cleaned test directory");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static Consumer<ChronoDBTransaction> waitForSeconds(final int seconds, final String text) {
		return tx -> {
			try {
				if (text != null) {
					System.out.println(text);
				}
				System.out.println("Waiting for " + seconds + " seconds...");
				Thread.sleep(seconds * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		};
	}
}
