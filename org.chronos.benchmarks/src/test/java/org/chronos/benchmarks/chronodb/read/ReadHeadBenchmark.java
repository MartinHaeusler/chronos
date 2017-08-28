package org.chronos.benchmarks.chronodb.read;

import static org.chronos.common.logging.ChronoLogger.*;

import java.util.List;

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
 * calls are executed on the head revision for this very key. This forms a list of samples, where each sample reflects
 * how long it took to read the head revision, based on the number of versions on this key.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
@Category(PerformanceTest.class)
public class ReadHeadBenchmark extends AllChronoDBBackendsTest {

	@Test
	public void writeVersionTimes1000readHeadTimes10() {
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
		logInfo("Starting Read Head Benchmark on backend [" + this.getChronoBackendName() + "].");
		logInfo("Performing " + writes + " writes, for each write reading the head revision " + reads + " times");
		logInfo("");
		logInfo("Iteration\t\tAverage\t\tStandard Deviation");
		for (int iteration = 0; iteration < writes; iteration++) {
			NamedPayload value = values.get(iteration);
			ChronoDBTransaction tx = db.tx();
			tx.put(key, value);
			tx.commit();
			TimeStatistics statistics = Measure.multipleTimes(reads, false, () -> {
				NamedPayload readValue = tx.get(key);
				if (readValue.getName().equals(value.getName()) == false) {
					throw new AssertionError("Put/Get are not in synch!");
				}
			});
			double avg = statistics.getAverageRuntime();
			double std = statistics.getStandardDeviation();
			logInfo(iteration + "\t\t" + avg + "\t\t" + std);
		}
		logInfo("");
		logInfo("End of run.");
	}

}
