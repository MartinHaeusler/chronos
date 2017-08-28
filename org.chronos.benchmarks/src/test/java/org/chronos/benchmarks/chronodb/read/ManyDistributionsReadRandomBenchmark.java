package org.chronos.benchmarks.chronodb.read;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

import org.chronos.benchmarks.util.BenchmarkUtils;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpEntry;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.utils.Measure;
import org.chronos.common.test.utils.TimeStatistics;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicDouble;

/**
 * This benchmark measures the performance of {@link ChronoDB} in the scenario of having X keys with Y versions each,
 * where X*Y gives the configurable entry set.
 *
 * <p>
 * A random key set of a given size is generated and inserted at one given timestamp. Then, reads are executed at random
 * keys, and the performance of these reads is measured.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
@Category(PerformanceTest.class)
public class ManyDistributionsReadRandomBenchmark extends AllChronoDBBackendsTest {

	private static final int NUMBER_OF_ENTRIES = 100_000;
	private static final int NUMBER_OF_READS = 10_000;

	private static final int NUMBER_OF_TEST_RUNS = 5;
	private static final int NUMBER_OF_MEASURES_PER_RUN = 500;

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "false")
	public void performReadBenchmark() {
		// access the DB once to initialize it
		this.getChronoDB();
		for (int testRunIndex = 0; testRunIndex < NUMBER_OF_TEST_RUNS; testRunIndex++) {
			double percent = testRunIndex / ((double) NUMBER_OF_TEST_RUNS - 1);
			int keySetSize = (int) Math.round(NUMBER_OF_ENTRIES * (1.0 - percent));
			// assert that we have at least one key
			keySetSize = Math.max(1, keySetSize);
			TimeStatistics statistics = this.runTest(NUMBER_OF_ENTRIES, keySetSize, NUMBER_OF_READS,
					NUMBER_OF_MEASURES_PER_RUN);
			ChronoLogger.logInfo("Finished test #" + testRunIndex + " (get() x " + NUMBER_OF_READS + ") with "
					+ keySetSize + " keys and " + (NUMBER_OF_ENTRIES - keySetSize) + " version entries.");
			ChronoLogger.logInfo(statistics.toFullString());
		}

	}

	private TimeStatistics runTest(final int entrySetSize, final int keySetSize, final int reads, final int repeats) {
		ChronoDB db = this.reinstantiateDB();

		ChronoLogger.log("Preparing data...");
		Set<ChronoDBDumpEntry<?>> dataset = BenchmarkUtils.createRandomDataset(entrySetSize, keySetSize).build();
		this.checkDatasetConsistency(dataset);
		File dumpFile = new File(this.getTestDirectory(), "testdata.chronodump");
		BenchmarkUtils.writeDBDump(dumpFile, dataset);

		ChronoLogger.log("Filling database...");
		db.readDump(dumpFile);

		String branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
		String keyspace = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;
		List<String> keySetList = Lists.newArrayList(this.extractKeySet(dataset, branch, keyspace));

		long timeFrom = 0;
		long timeTo = this.getLatestTimestamp(dataset, branch);

		ChronoLogger.log("Starting read process...");

		// we keep track of the retrieved sum so that JavaC doesn't optimize away our reads
		AtomicDouble sum = new AtomicDouble(0.0);
		// perform the reads
		TimeStatistics statistics = Measure.multipleTimes(repeats, false, (measureNumber) -> {
			long requestTimestamp = BenchmarkUtils.randomBetween(timeFrom, timeTo);
			// ChronoLogger.log("Requesting timestamp: " + requestTimestamp);
			ChronoDBTransaction tx = db.tx(requestTimestamp);
			for (int iteration = 0; iteration < reads; iteration++) {
				int i = BenchmarkUtils.randomBetween(0, keySetSize - 1);
				String key = keySetList.get(i);
				Double value = (Double) tx.get(key);
				if (value == null) {
					value = 0.0;
				}
				sum.getAndAdd(value);
			}
		});
		// print the sum so that JavaC doesn't optimize away our reads
		ChronoLogger.logInfo("Retrieved sum: " + sum);
		return statistics;
	}

	private Set<String> extractKeySet(final Set<ChronoDBDumpEntry<?>> dump, final String branch,
			final String keyspace) {
		Set<String> keySet = Sets.newHashSet();
		for (ChronoDBDumpEntry<?> entry : dump) {
			ChronoIdentifier identifier = entry.getChronoIdentifier();
			if (identifier.getBranchName().equals(branch) && identifier.getKeyspace().equals(keyspace)) {
				String key = entry.getChronoIdentifier().getKey();
				keySet.add(key);
			}
		}
		return keySet;
	}

	private long getLatestTimestamp(final Set<ChronoDBDumpEntry<?>> dump, final String branch) {
		OptionalLong max = dump.parallelStream()
				.filter(entry -> entry.getChronoIdentifier().getBranchName().equals(branch))
				.mapToLong(entry -> entry.getChronoIdentifier().getTimestamp()).max();
		if (max.isPresent()) {
			return max.getAsLong();
		} else {
			return 0;
		}
	}

	private void checkDatasetConsistency(final Set<ChronoDBDumpEntry<?>> dataset) {
		SetMultimap<String, Long> keyToTimestamp = HashMultimap.create();
		for (ChronoDBDumpEntry<?> entry : dataset) {
			String key = entry.getChronoIdentifier().getKey();
			long timestamp = entry.getChronoIdentifier().getTimestamp();
			assertFalse(keyToTimestamp.containsEntry(key, timestamp));
			keyToTimestamp.put(key, timestamp);
		}
	}

}
