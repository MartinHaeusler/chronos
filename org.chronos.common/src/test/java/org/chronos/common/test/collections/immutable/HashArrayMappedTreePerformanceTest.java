package org.chronos.common.test.collections.immutable;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.chronos.common.collections.immutable.ImmutableMap;
import org.chronos.common.collections.immutable.ImmutableMaps;
import org.junit.Test;

import com.google.common.collect.Maps;

public class HashArrayMappedTreePerformanceTest {

	@Test
	public void readWritePerformanceTest() {
		// config
		int dictionarySize = 100_000;

		System.out.println("Starting in 5 seconds.");
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
		}
		System.out.println("GO!");

		Map<String, String> dictionary = Maps.newHashMap();
		for (int i = 0; i < dictionarySize; i++) {
			String key = UUID.randomUUID().toString();
			String value = UUID.randomUUID().toString();
			dictionary.put(key, value);
		}
		Map<String, String> hashMap = Maps.newHashMap();
		Map<String, String> treeMap = Maps.newTreeMap();
		ImmutableMap<String, String> hamtMap = ImmutableMaps.newHashArrayMappedTreeMap();

		{ // WRITE PART
			{ // hash map
				long timeBefore = System.currentTimeMillis();
				for (Entry<String, String> entry : dictionary.entrySet()) {
					hashMap.put(entry.getKey(), entry.getValue());
				}
				long timeAfter = System.currentTimeMillis();
				System.out.println("INSERTION :: java.util.HashMap -> " + (timeAfter - timeBefore) + "ms");
			}

			{ // tree map
				long timeBefore = System.currentTimeMillis();
				for (Entry<String, String> entry : dictionary.entrySet()) {
					treeMap.put(entry.getKey(), entry.getValue());
				}
				long timeAfter = System.currentTimeMillis();
				System.out.println("INSERTION :: java.util.TreeMap -> " + (timeAfter - timeBefore) + "ms");
			}

			{ // hamt map
				long timeBefore = System.currentTimeMillis();
				for (Entry<String, String> entry : dictionary.entrySet()) {
					hamtMap = hamtMap.put(entry.getKey(), entry.getValue());
				}
				long timeAfter = System.currentTimeMillis();
				System.out.println("INSERTION :: HashArrayMappedTreeMap -> " + (timeAfter - timeBefore) + "ms");
			}
		}

		{ // READ PART

			{ // hash map GET
				long timeBeforeGetTest = System.currentTimeMillis();
				for (Entry<String, String> entry : dictionary.entrySet()) {
					assertEquals(entry.getValue(), hashMap.get(entry.getKey()));
				}
				long timeAfterGetTest = System.currentTimeMillis();
				System.out.println("GET :: java.util.HashMap -> " + (timeAfterGetTest - timeBeforeGetTest) + "ms");
			}

			{ // tree map GET
				long timeBefore = System.currentTimeMillis();
				for (Entry<String, String> entry : dictionary.entrySet()) {
					assertEquals(entry.getValue(), treeMap.get(entry.getKey()));
				}
				long timeAfter = System.currentTimeMillis();
				System.out.println("GET :: java.util.TreeMap -> " + (timeAfter - timeBefore) + "ms");
			}

			{ // HAMT map GET
				long timeBefore = System.currentTimeMillis();
				for (Entry<String, String> entry : dictionary.entrySet()) {
					assertEquals(entry.getValue(), hamtMap.get(entry.getKey()));
				}
				long timeAfter = System.currentTimeMillis();
				System.out.println("GET :: HAMT Map -> " + (timeAfter - timeBefore) + "ms");
			}

			{ // hash map ITER
				long timeBefore = System.currentTimeMillis();
				long entries = 0;
				for (int i = 0; i < 100; i++) {
					for (Entry<String, String> entry : hashMap.entrySet()) {
						if (entry == null) {
							throw new IllegalStateException();
						}
						entries++;
					}
				}
				assertEquals(100 * dictionary.size(), entries);
				long timeAfter = System.currentTimeMillis();
				System.out.println("ITER :: java.util.HashMap -> " + (timeAfter - timeBefore) + "ms");
			}

			{ // tree map ITER
				long timeBefore = System.currentTimeMillis();
				long entries = 0;
				for (int i = 0; i < 100; i++) {
					for (Entry<String, String> entry : treeMap.entrySet()) {
						if (entry == null) {
							throw new IllegalStateException();
						}
						entries++;
					}
				}
				assertEquals(100 * dictionary.size(), entries);
				long timeAfter = System.currentTimeMillis();
				System.out.println("ITER :: java.util.TreeMap -> " + (timeAfter - timeBefore) + "ms");
			}

			{ // hamt map ITER
				long timeBefore = System.currentTimeMillis();
				long entries = 0;
				for (int i = 0; i < 100; i++) {
					for (Entry<String, String> entry : hamtMap.entrySet()) {
						if (entry == null) {
							throw new IllegalStateException();
						}
						entries++;
					}
				}
				assertEquals(100 * dictionary.size(), entries);
				long timeAfter = System.currentTimeMillis();
				System.out.println("ITER :: HashArrayMappedTreeMap -> " + (timeAfter - timeBefore) + "ms");
			}

		}
		System.out.println("Done.");
	}

	@Test
	public void iterationPerformanceTest() {
		// config
		int dictionarySize = 100_000;

		Map<String, String> dictionary = Maps.newHashMap();
		for (int i = 0; i < dictionarySize; i++) {
			String key = UUID.randomUUID().toString();
			String value = UUID.randomUUID().toString();
			dictionary.put(key, value);
		}
		ImmutableMap<String, String> hamtMap = ImmutableMaps.newHashArrayMappedTreeMap();

		{ // write
			long timeBefore = System.currentTimeMillis();
			for (Entry<String, String> entry : dictionary.entrySet()) {
				hamtMap = hamtMap.put(entry.getKey(), entry.getValue());
			}
			long timeAfter = System.currentTimeMillis();
			System.out.println("INSERTION :: HashArrayMappedTreeMap -> " + (timeAfter - timeBefore) + "ms");
		}

		long delaySec = 5;

		System.out.println("Starting iteration in " + delaySec + " seconds.");
		try {
			Thread.sleep(delaySec * 1000);
		} catch (InterruptedException e) {
		}
		System.out.println("GO!");

		{ // ITER
			long timeBefore = System.currentTimeMillis();
			long entries = 0;
			int iterations = 1000;
			for (int i = 0; i < iterations; i++) {
				for (Entry<String, String> entry : hamtMap.entrySet()) {
					if (entry == null) {
						throw new IllegalStateException();
					}
					entries++;
				}
			}
			assertEquals(iterations * dictionary.size(), entries);
			long timeAfter = System.currentTimeMillis();
			System.out.println("ITER :: HashArrayMappedTreeMap -> " + (timeAfter - timeBefore) + "ms");
		}

	}

}
