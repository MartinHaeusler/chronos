package org.chronos.benchmarks.chronodb.secondaryindexing;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.chronos.benchmarks.util.BenchmarkUtils;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.utils.Measure;
import org.chronos.common.test.utils.Statistic;
import org.chronos.common.test.utils.TimeStatistics;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

@Category(PerformanceTest.class)
public class SecondaryIndexingBenchmarkForEnums extends AllChronoDBBackendsTest {

	private static final int ENTRIES = 100_000;
	private static final int KEY_SET_SIZE = 5000;
	private static final int COMMIT_SIZE = 200;

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.ASSUME_CACHE_VALUES_ARE_IMMUTABLE, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.DUPLICATE_VERSION_ELIMINATION_MODE, value = "off")
	@InstantiateChronosWith(property = ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY, value = "OVERWRITE_WITH_SOURCE")
	public void benchmarkEnumIndexingReadHead() {
		ChronoDB db = this.getChronoDB();
		IndexManager indexManager = db.getIndexManager();
		StringIndexer typeIndexer = new TestBeanTypeIndexer();
		indexManager.addIndexer("type", typeIndexer);
		// generate the key set (as a list)
		List<String> keySetAsList = BenchmarkUtils.randomKeySetAsList(KEY_SET_SIZE);
		int writtenEntries = 0;
		int iteration = 0;
		while (writtenEntries < ENTRIES) {
			// generate the next batch of entries
			Set<TestBean> entries = BenchmarkUtils.generateValuesSet(TestBean::new, COMMIT_SIZE);
			// create a new transaction and add the entries to random keys within the key set
			ChronoDBTransaction tx = db.tx();
			// we need to make sure no key is written twice
			Set<String> usedKeys = Sets.newHashSet();
			for (TestBean bean : entries) {
				String key = null;
				do {
					key = BenchmarkUtils.getRandomEntryOf(keySetAsList);
				} while (usedKeys.contains(key));
				tx.put(key, bean);
			}
			Measure.startTimeMeasure("commit");
			tx.commit();
			long commitTime = Measure.endTimeMeasure("commit");
			writtenEntries += COMMIT_SIZE;
			iteration++;
			ChronoLogger.log("Commit #" + iteration + " took " + commitTime + "ms");

			// perform the actual reads
			TimeStatistics statistics = new TimeStatistics();
			{
				statistics.beginRun();
				Set<QualifiedKey> set1 = tx.find().inDefaultKeyspace().where("type").isEqualTo(BeanType.ONE.toString())
						.getKeysAsSet();
				long readTime1 = statistics.endRun();
				ChronoLogger.log("Read 1 took " + readTime1 + "ms and produced " + set1.size() + " elements.");
			}
			{
				statistics.beginRun();
				Set<QualifiedKey> set2 = tx.find().inDefaultKeyspace().where("type").isEqualTo(BeanType.TWO.toString())
						.getKeysAsSet();
				long readTime2 = statistics.endRun();
				ChronoLogger.log("Read 2 took " + readTime2 + "ms and produced " + set2.size() + " elements.");
			}
			{
				statistics.beginRun();
				Set<QualifiedKey> set3 = tx.find().inDefaultKeyspace().where("type")
						.isEqualTo(BeanType.THREE.toString()).getKeysAsSet();
				long readTime3 = statistics.endRun();
				ChronoLogger.log("Read 3 took " + readTime3 + "ms and produced " + set3.size() + " elements.");
			}
			ChronoLogger.log("Iteration #" + iteration + " read time statistics: " + statistics.toFullString());
		}
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.ASSUME_CACHE_VALUES_ARE_IMMUTABLE, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.DUPLICATE_VERSION_ELIMINATION_MODE, value = "off")
	@InstantiateChronosWith(property = ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY, value = "OVERWRITE_WITH_SOURCE")
	@InstantiateChronosWith(property = ChronoDBConfiguration.JDBC_CONNECTION_URL, value = "jdbc:h2:file:${testdir}/h2")
	public void benchmarkEnumIndexingReadRandom() {
		ChronoDB db = this.getChronoDB();
		IndexManager indexManager = db.getIndexManager();
		StringIndexer typeIndexer = new TestBeanTypeIndexer();
		indexManager.addIndexer("type", typeIndexer);
		// generate the key set (as a list)
		List<String> keySetAsList = BenchmarkUtils.randomKeySetAsList(KEY_SET_SIZE);
		int writtenEntries = 0;
		int iteration = 0;
		long firstTimestamp = -1;
		long maxTimestamp = -1;
		Statistic readStatistic = new Statistic();
		while (writtenEntries < ENTRIES) {
			// generate the next batch of entries
			Set<TestBean> entries = BenchmarkUtils.generateValuesSet(TestBean::new, COMMIT_SIZE);
			// create a new transaction and add the entries to random keys within the key set
			ChronoDBTransaction tx = db.tx();
			// we need to make sure no key is written twice
			Set<String> usedKeys = Sets.newHashSet();
			for (TestBean bean : entries) {
				String key = null;
				do {
					key = BenchmarkUtils.getRandomEntryOf(keySetAsList);
				} while (usedKeys.contains(key));
				tx.put(key, bean);
			}
			Measure.startTimeMeasure("commit");
			tx.commit();
			long commitTime = Measure.endTimeMeasure("commit");
			writtenEntries += COMMIT_SIZE;
			iteration++;
			// update the time bounds of the dataset to choose from
			if (firstTimestamp < 0) {
				firstTimestamp = tx.getTimestamp();
			}
			maxTimestamp = tx.getTimestamp();
			ChronoLogger.log("Commit #" + iteration + " took " + commitTime + "ms");

			// choose the timestamp to read from, and open the transaction on this timestamp
			long timestamp = BenchmarkUtils.randomBetween(firstTimestamp, maxTimestamp);
			ChronoDBTransaction readTx = db.tx(timestamp);
			ChronoLogger.log("Reading on timestamp " + timestamp + " (" + (timestamp - firstTimestamp)
					+ "ms after initial commit)");
			// perform the actual reads
			TimeStatistics statistics = new TimeStatistics();
			{
				statistics.beginRun();
				Set<QualifiedKey> set1 = readTx.find().inDefaultKeyspace().where("type")
						.isEqualTo(BeanType.ONE.toString()).getKeysAsSet();
				long readTime1 = statistics.endRun();
				ChronoLogger.log("Read 1 took " + readTime1 + "ms and produced " + set1.size() + " elements.");
			}
			{
				statistics.beginRun();
				Set<QualifiedKey> set2 = readTx.find().inDefaultKeyspace().where("type")
						.isEqualTo(BeanType.TWO.toString()).getKeysAsSet();
				long readTime2 = statistics.endRun();
				ChronoLogger.log("Read 2 took " + readTime2 + "ms and produced " + set2.size() + " elements.");
			}
			{
				statistics.beginRun();
				Set<QualifiedKey> set3 = readTx.find().inDefaultKeyspace().where("type")
						.isEqualTo(BeanType.THREE.toString()).getKeysAsSet();
				long readTime3 = statistics.endRun();
				ChronoLogger.log("Read 3 took " + readTime3 + "ms and produced " + set3.size() + " elements.");
			}
			ChronoLogger.log("Iteration #" + iteration + " read time statistics: " + statistics.toFullString());
			readStatistic.addSample(statistics.getAverageRuntime());
		}
		ChronoLogger.log("Read Time Averages");
		DecimalFormat format = new DecimalFormat("0.00");
		for (Double sample : readStatistic.getSamples()) {
			ChronoLogger.log("\t" + format.format(sample));
		}
	}

	private static class TestBean {

		private BeanType beanType;

		private TestBean() {
			double random = Math.random();
			if (random <= 0.33) {
				this.beanType = BeanType.ONE;
			} else if (random <= 0.66) {
				this.beanType = BeanType.TWO;
			} else {
				this.beanType = BeanType.THREE;
			}
		}

		public BeanType getType() {
			return this.beanType;
		}

	}

	private static enum BeanType {

		ONE, TWO, THREE;

	}

	private static class TestBeanTypeIndexer implements StringIndexer {

		@Override
		public boolean canIndex(final Object object) {
			return object != null && object instanceof TestBean;
		}

		@Override
		public Set<String> getIndexValues(final Object object) {
			TestBean testBean = (TestBean) object;
			return Collections.singleton(testBean.getType().toString());
		}

	}
}
