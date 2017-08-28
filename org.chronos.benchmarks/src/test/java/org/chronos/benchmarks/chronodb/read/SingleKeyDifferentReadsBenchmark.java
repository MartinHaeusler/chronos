package org.chronos.benchmarks.chronodb.read;

import static org.chronos.common.logging.ChronoLogger.*;

import java.util.List;
import java.util.Stack;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.utils.Measure;
import org.chronos.common.test.utils.TimeStatistics;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

/**
 * This benchmark measures the impact of versioning on read performance.
 *
 * <p>
 * The general idea is that there's a growing backlog of older versions, and due to how the B-Trees are sorted in
 * ChronoDB, it is safe to assume that performance will decrease as more versions are added. This performance benchmark
 * exists to verify this assumption, and to judge how bad the decrease over time really is.
 *
 * <p>
 * The test focuses on a single key, which is overwritten at several timestamps. After each n-th overwrite, a number of
 * read calls are executed on a random revision for this very key. This forms a list of samples, where each sample
 * reflects how long it took to read the random revision, based on the number of versions on this key.
 *
 * <p>
 * This benchmark starts with an initially empty matrix, and writes only to a single key. For a benchmark that uses a
 * larger matrix with many keys (each with several versions) please refer to
 * {@link FixedDistributionReadRandomBenchmark}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
@Category(PerformanceTest.class)
public class SingleKeyDifferentReadsBenchmark extends AllChronoDBBackendsTest {

	@Test
	public void runBenchmark() {
		int writes = 110000;
		int reads = 1000;
		int sampleEveryXWrites = 10000;
		this.writeVersionTimesXreadHeadTimesY(writes, reads, sampleEveryXWrites);
	}

	private void writeVersionTimesXreadHeadTimesY(final int writes, final int reads, final int sampleEveryXWrites) {
		// prepare the benchmark data
		String key = "key";
		List<Double> values = Lists.newArrayList();
		ChronoDB db = this.getChronoDB();
		for (int i = 0; i < writes; i++) {
			double value = Math.random();
			values.add(value);
		}
		Long minTimestamp = null;
		Long maxTimestamp = null;
		logInfo("Starting Read Random Benchmark on backend [" + this.getChronoBackendName() + "].");
		logInfo("Performing " + writes + " writes, for each " + sampleEveryXWrites
				+ "th write reading a random revision " + reads + " times");
		logInfo("");
		logInfo("Iteration\t\tAverage\t\tStandard Deviation");
		for (int iteration = 0; iteration < writes; iteration++) {
			Double value = values.get(iteration);
			ChronoDBTransaction tx = db.tx();
			tx.put(key, value);
			tx.commit();
			if (minTimestamp == null) {
				minTimestamp = tx.getTimestamp();
			}
			maxTimestamp = tx.getTimestamp();
			// generate a random timestamp to load at each read
			Stack<Long> randomTimestamps = new Stack<Long>();
			for (int i = 0; i < reads * 500; i++) {
				long timestamp = (long) Math
						.floor(Math.floor(Math.random() * (maxTimestamp - minTimestamp)) + minTimestamp);
				randomTimestamps.add(timestamp);
			}
			if (iteration % sampleEveryXWrites == 0) {
				TimeStatistics statistics = Measure.multipleTimes(500, false, () -> {
					for (int read = 0; read < reads; read++) {
						long timestamp = randomTimestamps.pop();
						@SuppressWarnings("unused")
						Double readValue = db.tx(timestamp).get(key);
					}
				});
				double avg = statistics.getAverageRuntime();
				double std = statistics.getStandardDeviation();
				logInfo(iteration + "\t\t" + avg + "\t\t" + std);
			}
		}
		logInfo("");
		logInfo("End of run.");
	}

}
