package org.chronos.benchmarks.chronodb.concurrency;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.chronos.chronodb.api.ChangeSetEntry;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class ConcurrentChronoDBAccessTest extends AllChronoDBBackendsTest {

	private static final int NUMBER_OF_WRITERS = 10;
	private static final int NUMBER_OF_READERS = 500;

	private static final int NUMBER_OF_KEYSPACES = 5;
	private static final int KEY_SET_SIZE = 10_000;

	private static final int COMMIT_SIZE = 500;
	private static final int COMMITS_PER_WRITER = 200;
	private static final double CHANCE_FOR_REMOVE = 0.05;

	private static final int ITERATIONS_PER_READER = 1000;
	private static final int READS_PER_READER_ITERATION = 40;

	private static final long WRITER_INTERVAL = 10;
	private static final long WRITER_INTERVAL_VARIANCE = 3;

	private static final long READER_INTERVAL = 3;
	private static final long READER_INTERVAL_VARIANCE = 1;

	private List<String> allKeyspaces = this.generateKeyspaceNames(NUMBER_OF_KEYSPACES);
	private List<String> allKeys = this.generateKeys(KEY_SET_SIZE);

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.ENABLE_BLIND_OVERWRITE_PROTECTION, value = "false")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
	@InstantiateChronosWith(property = ChronoDBConfiguration.ASSUME_CACHE_VALUES_ARE_IMMUTABLE, value = "true")
	public void chronoDbWorksCorrectlyUnderConcurrentReadWriteAccess() {
		ChronoDB db = this.getChronoDB();
		Set<Thread> writerThreads = Sets.newHashSet();
		Set<Thread> readerThreads = Sets.newHashSet();
		// create the writers
		for (int i = 0; i < NUMBER_OF_WRITERS; i++) {
			List<Set<ChangeSetEntry>> changeSets = this.generateChangeSets(this.allKeyspaces, this.allKeys);
			WriterTask task = new WriterTask(db, changeSets, WRITER_INTERVAL, WRITER_INTERVAL_VARIANCE);
			Thread writerThread = new Thread(task);
			writerThread.setName("Writer" + i);
			writerThreads.add(writerThread);
		}
		// create the readers
		for (int i = 0; i < NUMBER_OF_READERS; i++) {
			ReaderTask task = new ReaderTask(db, this::readerAssertFunction, ITERATIONS_PER_READER, READER_INTERVAL,
					READER_INTERVAL_VARIANCE);
			Thread readerThread = new Thread(task);
			readerThread.setName("Reader" + i);
			readerThreads.add(readerThread);
		}
		// mix reader and writer starts
		List<Thread> allWorkerThreads = Lists.newArrayList();
		allWorkerThreads.addAll(writerThreads);
		allWorkerThreads.addAll(readerThreads);
		Collections.shuffle(allWorkerThreads);
		// start the party!
		allWorkerThreads.parallelStream().forEach(thread -> thread.start());
		// ... and wait for it to finish
		allWorkerThreads.forEach(thread -> {
			try {
				thread.join();
			} catch (InterruptedException ignored) {
			}
		});
		// assert that the DB is non-empty
		BranchInternal masterBranch = (BranchInternal) db.getBranchManager()
				.getBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
		TemporalKeyValueStore keyValueStore = masterBranch.getTemporalKeyValueStore();
		assertTrue(keyValueStore.getNow() > 0);
	}

	private List<String> generateKeyspaceNames(final int size) {
		List<String> keyspaceNames = Lists.newArrayList();
		for (int i = 0; i < size; i++) {
			keyspaceNames.add("KS" + i);
		}
		return Collections.unmodifiableList(keyspaceNames);
	}

	private List<String> generateKeys(final int size) {
		List<String> allKeys = Lists.newArrayList();
		for (int i = 0; i < size; i++) {
			allKeys.add("K" + i);
		}
		return Collections.unmodifiableList(allKeys);
	}

	private List<Set<ChangeSetEntry>> generateChangeSets(final List<String> allKeyspaces, final List<String> allKeys) {
		List<Set<ChangeSetEntry>> changeSets = Lists.newArrayList();
		for (int commitIndex = 0; commitIndex < COMMITS_PER_WRITER; commitIndex++) {
			Set<ChangeSetEntry> changeSet = this.generateChangeSet(allKeyspaces, allKeys);
			changeSets.add(changeSet);
		}
		return Collections.unmodifiableList(changeSets);
	}

	private Set<ChangeSetEntry> generateChangeSet(final List<String> allKeyspaces, final List<String> allKeys) {
		Set<ChangeSetEntry> changeSet = Sets.newHashSet();
		for (int changeIndex = 0; changeIndex < COMMIT_SIZE; changeIndex++) {
			// choose a random keyspace
			String keyspace = this.chooseRandomEntryFrom(allKeyspaces);
			String key = this.chooseRandomEntryFrom(allKeys);
			QualifiedKey qKey = QualifiedKey.create(keyspace, key);
			ChangeSetEntry entry = null;
			if (Math.random() <= CHANCE_FOR_REMOVE) {
				// it's a "remove X" command
				entry = ChangeSetEntry.createDeletion(qKey);
			} else {
				// it's a "set to X" command
				int value = this.generateRandomInteger();
				entry = ChangeSetEntry.createChange(qKey, value);
			}
			changeSet.add(entry);
		}
		return Collections.unmodifiableSet(changeSet);
	}

	private void readerAssertFunction(final ChronoDBTransaction tx) {
		for (int i = 0; i < READS_PER_READER_ITERATION; i++) {
			// choose a random keyspace and key
			String keyspace = this.chooseRandomEntryFrom(this.allKeyspaces);
			String key = this.chooseRandomEntryFrom(this.allKeys);
			Integer value = (Integer) tx.get(keyspace, key);
			// we are happy if the value is null (i.e. was removed or has never been present) or non-negative
			if (value == null) {
				// pass
			} else {
				assertTrue(value >= 0);
			}
		}
	}

	private <T> T chooseRandomEntryFrom(final List<T> list) {
		checkNotNull(list, "Precondition violation - argument 'list' must not be NULL!");
		checkArgument(list.isEmpty() == false,
				"Precondition violation - cannot select random element from empty list!");
		int index = (int) Math.round(Math.random() * (list.size() - 1));
		return list.get(index);
	}

	private int generateRandomInteger() {
		return (int) Math.floor(Math.random() * Integer.MAX_VALUE);
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	private static abstract class IntervalTask implements Runnable {

		private final long interval;
		private final long intervalVariance;

		protected IntervalTask(final long interval, final long intervalVariance) {
			checkArgument(interval > 0, "Precndition violation - argument 'interval' must be greater than zero!");
			this.interval = interval;
			this.intervalVariance = intervalVariance;
		}

		protected long getNextInterval() {
			if (this.intervalVariance == 0) {
				return this.interval;
			} else {
				int randomSignum = Math.random() >= 0.5 ? 1 : -1;
				return this.interval + Math.round(Math.random() * this.intervalVariance) * randomSignum;
			}
		}

	}

	private static class WriterTask extends IntervalTask {

		private final ChronoDB db;

		private final List<Set<ChangeSetEntry>> changeSets;

		public WriterTask(final ChronoDB db, final List<Set<ChangeSetEntry>> changeSets, final long interval,
				final long intervalVariance) {
			super(interval, intervalVariance);
			checkNotNull(db, "Precondition violation - argument 'db' must not be NULL!");
			checkNotNull(changeSets, "Precondition violation - argument 'changeSets' must not be NULL!");
			this.db = db;
			this.changeSets = changeSets;
		}

		@Override
		public void run() {
			ChronoLogger.log("Writer Thread started: [" + Thread.currentThread().getName() + "]");
			for (int iteration = 0; iteration < this.changeSets.size(); iteration++) {
				if (iteration % 10 == 0) {
					ChronoLogger.log("Writer Thread [" + Thread.currentThread().getName()
							+ "] is performing iteration #" + iteration + " of " + this.changeSets.size());
				}
				Set<ChangeSetEntry> changeSet = this.changeSets.get(iteration);
				ChronoDBTransaction tx = this.db.tx();
				for (ChangeSetEntry entry : changeSet) {
					String keyspace = entry.getKeyspace();
					String key = entry.getKey();
					Object value = entry.getValue();
					if (entry.isRemove()) {
						tx.remove(keyspace, key);
					} else {
						tx.put(keyspace, key, value);
					}
				}
				tx.commit();
				long waitInterval = this.getNextInterval();
				try {
					Thread.sleep(waitInterval);
				} catch (InterruptedException e) {
					ChronoLogger.logWarning("Writer Task interrupted in iteration #" + iteration + "!");
					return;
				}
			}
			ChronoLogger.log("Writer Thread finished: [" + Thread.currentThread().getName() + "]");
		}

	}

	private static class ReaderTask extends IntervalTask {

		private final ChronoDB db;
		private final long repeats;
		private final Consumer<ChronoDBTransaction> assertFunction;

		public ReaderTask(final ChronoDB db, final Consumer<ChronoDBTransaction> assertFunction, final long repeats,
				final long interval, final long intervalVariance) {
			super(interval, intervalVariance);
			checkNotNull(db, "Precondition violation - argument 'db' must not be NULL!");
			checkNotNull(assertFunction, "Precondition violation - argument 'assertFunction' must not be NULL!");
			checkArgument(repeats > 0, "Precondition violation - argument 'repeats' must be greater than zero!");
			this.db = db;
			this.assertFunction = assertFunction;
			this.repeats = repeats;
		}

		@Override
		public void run() {
			ChronoLogger.log("Reader Thread started: [" + Thread.currentThread().getName() + "]");
			for (int iteration = 0; iteration < this.repeats; iteration++) {
				if (iteration % 10 == 0) {
					ChronoLogger.log("Reader Thread [" + Thread.currentThread().getName()
							+ "] is performing iteration #" + iteration + " of " + this.repeats);
				}
				ChronoDBTransaction tx = this.db.tx();
				this.assertFunction.accept(tx);
				long waitInterval = this.getNextInterval();
				try {
					Thread.sleep(waitInterval);
				} catch (InterruptedException e) {
					ChronoLogger.logWarning("Reader Task interrupted in iteration #" + iteration + "!");
					return;
				}
			}
			ChronoLogger.log("Reader Thread finished: [" + Thread.currentThread().getName() + "]");
		}

	}

}
