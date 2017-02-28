package org.chronos.chronodb.test.engine.transaction;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplUtils;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronodb.test.util.TestUtils;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@Category(PerformanceTest.class)
public class IncrementalCommitPerformanceTest extends AllChronoDBBackendsTest {

	@Test
	public void massiveIncrementalCommitsProduceConsistentStoreWithBatchInsert() {
		Set<String> supportedBackends = Sets.newHashSet(ChronosBackend.TUPL.toString(), ChronosBackend.CHUNKDB.toString());
		Assume.assumeTrue(supportedBackends.contains(this.getChronoBackendName()));
		this.runMassiveIncrementalCommitTest(true);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10000")
	public void massiveIncrementalCommitsProduceConsistentStoreWithBatchInsertAndCache() {
		Set<String> supportedBackends = Sets.newHashSet(ChronosBackend.TUPL.toString(), ChronosBackend.CHUNKDB.toString());
		Assume.assumeTrue(supportedBackends.contains(this.getChronoBackendName()));
		this.runMassiveIncrementalCommitTest(true);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10000")
	public void massiveIncrementalCommitsProduceConsistentStoreWithRegularInsertAndCache() {
		Set<String> supportedBackends = Sets.newHashSet(ChronosBackend.TUPL.toString(), ChronosBackend.CHUNKDB.toString());
		Assume.assumeTrue(supportedBackends.contains(this.getChronoBackendName()));
		this.runMassiveIncrementalCommitTest(false);
	}

	@Test
	public void massiveIncrementalCommitsProduceConsistentStoreWithRegularInsert() {
		Set<String> supportedBackends = Sets.newHashSet(ChronosBackend.TUPL.toString(), ChronosBackend.CHUNKDB.toString());
		Assume.assumeTrue(supportedBackends.contains(this.getChronoBackendName()));
		this.runMassiveIncrementalCommitTest(false);
	}

	private void runMassiveIncrementalCommitTest(final boolean useBatch) {
		ChronoDB db = this.getChronoDB();

		// we want at least three batches
		final int keyCount = TuplUtils.BATCH_INSERT_THRESHOLD * 4;
		final int keyspaceCount = 2;

		List<String> keysList = Lists.newArrayList();
		List<String> keyspaces = Lists.newArrayList();
		Map<String, String> keyToKeyspace = Maps.newHashMap();
		for (int i = 0; i < keyspaceCount; i++) {
			keyspaces.add("ks" + i);
		}
		for (int i = 0; i < keyCount; i++) {
			String keyspace = TestUtils.getRandomEntryOf(keyspaces);
			String uuid = UUID.randomUUID().toString();
			keysList.add(uuid);
			keyToKeyspace.put(uuid, keyspace);
		}
		keysList = Collections.unmodifiableList(keysList);

		final int maxBatchSize;
		if (useBatch) {
			// we force batch inserts by choosing a size larger than the batch threshold
			maxBatchSize = TuplUtils.BATCH_INSERT_THRESHOLD + 1;
		} else {
			// we force normal inserts by choosing a size less than the batch threshold
			maxBatchSize = TuplUtils.BATCH_INSERT_THRESHOLD - 1;
		}

		ChronoDBTransaction tx = db.tx();

		int index = 0;
		int batchSize = 0;
		int batchCount = 0;
		while (index < keyCount) {
			String uuid = keysList.get(index);
			String keyspace = keyToKeyspace.get(uuid);
			tx.put(keyspace, uuid, uuid);
			index++;
			batchSize++;
			if (batchSize >= maxBatchSize) {
				tx.commitIncremental();
				batchSize = 0;
				batchCount++;
				for (int i = 0; i < index; i++) {
					String test = keysList.get(i);
					String ks = keyToKeyspace.get(test);
					try {
						assertEquals(test, tx.get(ks, test));
					} catch (AssertionError e) {
						System.out.println("Error occurred on Test\t\tBatch: " + batchCount + "\t\ti: " + i + "\t\tmaxIndex: " + index);
						throw e;
					}
				}
			}
		}
		tx.commit();
		// check that all elements are present in the old transaction
		for (String uuid : keysList) {
			String keyspace = keyToKeyspace.get(uuid);
			assertEquals(uuid, tx.get(keyspace, uuid));
		}

		// check that all elements are present in a new transaction
		ChronoDBTransaction tx2 = db.tx();
		for (String uuid : keysList) {
			String keyspace = keyToKeyspace.get(uuid);
			assertEquals(uuid, tx2.get(keyspace, uuid));
		}
	}
}
