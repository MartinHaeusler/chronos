package org.chronos.benchmarks.chronodb.read;

import static org.chronos.common.logging.ChronoLogger.*;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.chronos.benchmarks.util.RandomTemporalMatrixGenerator;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.internal.api.TemporalDataMatrix;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.utils.Measure;
import org.chronos.common.test.utils.Statistic;
import org.chronos.common.test.utils.TimeStatistics;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * This benchmark measures the impact of versioning on read performance.
 *
 * <p>
 * This benchmark measures the read performance of {@link ChronoDB} in a scenario where a large
 * {@link TemporalDataMatrix} has been loaded, where each key has several versions. The test itself consists of reading
 * the value for a random key at a random point in time.
 *
 * <p>
 * This test initially generates a DB dump containing the test data, and then <b>reloads</b> this dump on every
 * iteration to ensure that no caches can blur the test results.
 *
 * <p>
 * This benchmark focuses on a scenario with multiple keys and multiple versions. For a more specialized scenario
 * focussing solely on the cost of versioning, please see {@link SingleKeyReadRandomBenchmark} and {@link ReadHeadBenchmark}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
@Category(PerformanceTest.class)
public class FixedDistributionReadRandomBenchmark extends AllChronoDBBackendsTest {

	private static final int REPEATS = 10;
	private static final int KEY_SET_SIZE = 1000;
	private static final int VERSIONS_PER_KEY = 100;

	@Test
	public void read1000() {
		int entrySetSize = KEY_SET_SIZE * VERSIONS_PER_KEY;
		RandomTemporalMatrixGenerator generator = new RandomTemporalMatrixGenerator(1, entrySetSize);
		Set<String> keySet = Sets.newHashSet();
		for (int i = 0; i < KEY_SET_SIZE; i++) {
			keySet.add(UUID.randomUUID().toString());
		}
		generator.setKeys(keySet);
		File dumpFile = new File(this.getTestDirectory(), "Test.chronodump");
		generator.generate(dumpFile);
		logInfo("Dump file size: " + FileUtils.byteCountToDisplaySize(dumpFile.length()));
		this.runReadBenchmark(REPEATS, 1000, dumpFile, keySet);
	}

	private void runReadBenchmark(final int repeats, final int numberOfReads, final File dumpFile,
			final Set<String> keySet) {
		logInfo("Starting Read Benchmark on backend [" + this.getChronoBackendName() + "].");
		logInfo("Repeating " + repeats + " times: read " + numberOfReads + " times");
		Statistic statistic = new Statistic();
		for (int i = 0; i < repeats; i++) {
			logInfo("Loading Chrono Dump...");
			this.getChronoDB().readDump(dumpFile);
			logInfo("Chrono Dump loaded");
			List<String> keyList = Lists.newArrayList(keySet);
			Measure.startTimeMeasure("readTest");
			this.performReads(numberOfReads, this.getChronoDB(), keyList);
			long duration = Measure.endTimeMeasure("readTest");
			statistic.addSample(duration);
			this.reinstantiateDB();
			logInfo("Run #" + (i + 1) + " complete.");
		}
		TimeStatistics timeStatistic = new TimeStatistics(statistic);
		logInfo(timeStatistic.toFullString());
		logInfo("Runtimes: " + statistic.getSamples().toString());
	}

	private void performReads(final int numberOfReads, final ChronoDB chronoDB, final List<String> keySet) {
		long now = chronoDB.tx().getTimestamp();
		Random random = new Random();
		for (int i = 0; i < numberOfReads; i++) {
			long timestamp = Math.round(random.nextDouble() * now);
			int keyIndex = (int) Math.round(random.nextDouble() * keySet.size());
			keyIndex = Math.max(0, Math.min(keyIndex, keySet.size() - 1));
			String key = keySet.get(keyIndex);
			chronoDB.tx(timestamp).get(key);
		}

	}

}
