package org.chronos.benchmarks.chronodb.read;

import java.util.List;
import java.util.Set;

import org.chronos.benchmarks.util.BenchmarkUtils;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.utils.Measure;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AtomicDouble;

/**
 * This benchmark measures the performance of {@link ChronoDB} in the (unrealistic) scenario of having many keys with
 * one version each.
 *
 * <p>
 * A random key set of a given size is generated and inserted at one given timestamp. Then, reads are executed at random
 * keys, and the performance of these reads is measured.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
@Category(PerformanceTest.class)
public class ManyKeysSingleVersionReadRandomBenchmark extends AllChronoDBBackendsTest {

	private static final int KEYSET_SIZE = 100_000;
	private static final int NUMBER_OF_READS = 1_000;

	private static final int NUMBER_OF_MEASURES = 500;

	@Test
	public void performReadBenchmark() {
		ChronoDB db = this.getChronoDB();

		// prepare a random keyset
		Set<String> keySet = BenchmarkUtils.randomKeySet(KEYSET_SIZE);

		ChronoLogger.log("Filling database...");

		// insert the keyset, with random doubles as values
		ChronoDBTransaction tx = db.tx();
		for (String key : keySet) {
			tx.put(key, Math.random());
		}
		tx.commit();

		List<String> keySetList = Lists.newArrayList(keySet);

		ChronoLogger.log("Starting read process...");

		// we keep track of the retrieved sum so that JavaC doesn't optimize away our reads
		AtomicDouble sum = new AtomicDouble(0.0);
		// perform the reads
		Measure.multipleTimes(NUMBER_OF_MEASURES, true, (measureNumber) -> {
			// ChronoLogger.logInfo("Running Measure #" + measureNumber);
			for (int iteration = 0; iteration < NUMBER_OF_READS; iteration++) {
				int i = BenchmarkUtils.randomBetween(0, KEYSET_SIZE - 1);
				String key = keySetList.get(i);
				sum.getAndAdd((double) tx.get(key));
			}
		});
		// print the sum so that JavaC doesn't optimize away our reads
		ChronoLogger.logInfo("Retrieved sum: " + sum);
	}

}
