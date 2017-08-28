package org.chronos.benchmarks.chronodb.write;

import static org.chronos.common.logging.ChronoLogger.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.utils.Measure;
import org.chronos.common.test.utils.Statistic;
import org.chronos.common.test.utils.TimeStatistics;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(PerformanceTest.class)
public class WriteOnlyBenchmark extends AllChronoDBBackendsTest {

	@Test
	public void write1000() {
		this.runWritePerformanceBenchmark(1000, 100, -1);
	}

	@Test
	public void write10000() {
		this.runWritePerformanceBenchmark(10000, 5, 1000);
	}

	private void runWritePerformanceBenchmark(final int numberOfEntries, final int repeats, final int batchSize) {
		logInfo("Starting Write-Only Benchmark on backend [" + this.getChronoBackendName() + "].");
		logInfo("Repeating " + repeats + " times: write " + numberOfEntries + " entries in batches of size " + batchSize);
		Statistic statistic = new Statistic();
		for (int i = 0; i < repeats; i++) {
			Measure.startTimeMeasure("writeTest");
			this.performWrite(numberOfEntries, batchSize, this.getChronoDB());
			long duration = Measure.endTimeMeasure("writeTest");
			statistic.addSample(duration);
			this.reinstantiateDB();
			logInfo("Run #" + (i + 1) + " complete.");
		}
		TimeStatistics timeStatistic = new TimeStatistics(statistic);
		logInfo(timeStatistic.toFullString());
		logInfo("Runtimes: " + statistic.getSamples().toString());
	}

	private void performWrite(final int numberOfEntries, final int batchSize, final ChronoDB db) {
		ChronoDBTransaction tx = db.tx();
		for (int i = 0; i < numberOfEntries; i++) {
			NamedPayload value = NamedPayload.create1KB();
			tx.put(value.getName(), value);
		}
		tx.commit();
	}
}
