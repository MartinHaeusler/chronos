package org.chronos.benchmarks.chronodb.opentx;

import static org.junit.Assert.*;

import java.io.IOException;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.utils.Measure;
import org.chronos.common.test.utils.TimeStatistics;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(PerformanceTest.class)
public class TransactionOpeningBenchmark extends AllChronoDBBackendsTest {

	@Test
	public void benchmarkTimeForTransactionOpening() throws IOException {
		ChronoDB db = this.getChronoDB();
		// System.out.println("Hit ENTER to start");
		// System.in.read();
		TimeStatistics statistics = Measure.multipleTimes(1000, true, () -> {
			for (int i = 0; i < 1000; i++) {
				ChronoDBTransaction tx = db.tx();
				assertNotNull(tx);
			}
		});
		ChronoLogger.logInfo(statistics.toFullString());
	}

}
