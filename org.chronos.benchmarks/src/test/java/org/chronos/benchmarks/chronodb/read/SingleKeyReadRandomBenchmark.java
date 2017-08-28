package org.chronos.benchmarks.chronodb.read;

import static org.chronos.common.logging.ChronoLogger.*;

import java.util.List;
import java.util.Stack;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
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
 * The test focuses on a single key, which is overwritten at several timestamps. After each overwrite, a number of read
 * calls are executed on a random revision for this very key. This forms a list of samples, where each sample reflects
 * how long it took to read the random revision, based on the number of versions on this key.
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
public class SingleKeyReadRandomBenchmark extends AllChronoDBBackendsTest {

	@Test
	public void writeVersionTimes1000readRandomTimes10() {
		int writes = 1000;
		int reads = 10;
		this.writeVersionTimesXreadHeadTimesY(writes, reads);
	}

	private void writeVersionTimesXreadHeadTimesY(final int writes, final int reads) {
		// prepare the benchmark data
		String key = "key";
		List<NamedPayload> values = Lists.newArrayList();
		ChronoDB db = this.getChronoDB();
		for (int i = 0; i < writes; i++) {
			NamedPayload value = NamedPayload.create1KB("Value " + i);
			values.add(value);
		}
		Long minTimestamp = null;
		Long maxTimestamp = null;
		logInfo("Starting Read Random Benchmark on backend [" + this.getChronoBackendName() + "].");
		logInfo("Performing " + writes + " writes, for each write reading a random revision " + reads + " times");
		logInfo("");
		logInfo("Iteration\t\tAverage\t\tStandard Deviation");
		for (int iteration = 0; iteration < writes; iteration++) {
			NamedPayload value = values.get(iteration);
			ChronoDBTransaction tx = db.tx();
			tx.put(key, value);
			tx.commit();
			if (minTimestamp == null) {
				minTimestamp = tx.getTimestamp();
			}
			maxTimestamp = tx.getTimestamp();
			// generate a random timestamp to load at each read
			Stack<Long> randomTimestamps = new Stack<Long>();
			for (int i = 0; i < reads; i++) {
				long timestamp = (long) Math
						.floor(Math.floor(Math.random() * (maxTimestamp - minTimestamp)) + minTimestamp);
				randomTimestamps.add(timestamp);
			}
			TimeStatistics statistics = Measure.multipleTimes(reads, false, () -> {
				long timestamp = randomTimestamps.pop();
				@SuppressWarnings("unused")
				NamedPayload readValue = db.tx(timestamp).get(key);
			});
			double avg = statistics.getAverageRuntime();
			double std = statistics.getStandardDeviation();
			logInfo(iteration + "\t\t" + avg + "\t\t" + std);
		}
		logInfo("");
		logInfo("End of run.");
	}

}
