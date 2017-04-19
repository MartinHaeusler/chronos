package org.chronos.common.test.collections.immutable;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.collections4.iterators.PermutationIterator;
import org.chronos.common.collections.immutable.ImmutableMap;
import org.chronos.common.collections.immutable.ImmutableMaps;
import org.chronos.common.collections.immutable.base.ImmutableEntry;
import org.chronos.common.collections.immutable.hamt.HAMTIndexNode;
import org.chronos.common.collections.immutable.hamt.HAMTLeafNode;
import org.chronos.common.collections.immutable.hamt.HAMTNode;
import org.chronos.common.collections.immutable.hamt.HashArrayMappedTree;
import org.chronos.common.collections.util.BitFieldUtil;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.serialization.KryoManager;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class HashArrayMappedTreeTest {

	@Test
	public void canCreateLeafNode() {
		HAMTNode<String, String> node = new HAMTLeafNode<>();
		assertNotNull(node);
	}

	@Test
	public void canCreateIndexNode() {
		HAMTNode<String, String> node = new HAMTIndexNode<>();
		assertNotNull(node);
	}

	@Test
	public void canGetAndSetLeafNodeValue() {
		HAMTLeafNode<String, String> node = new HAMTLeafNode<>();
		assertEquals(0, node.entryCount());
		HAMTLeafNode<String, String> helloWorldNode = node.putEntry(ImmutableEntry.of("Hello", "World"));
		assertEquals("Hello", helloWorldNode.getSingleEntry().getKey());
		assertEquals("World", helloWorldNode.getSingleEntry().getValue());
		assertEquals("World", helloWorldNode.getValueForKey(helloWorldNode.getSingleEntry().getKey()));
		assertEquals(0, node.entryCount());
	}

	@Test
	public void canAttachChildNode() {
		HAMTIndexNode<String, String> rootNode = new HAMTIndexNode<>();
		assertNotNull(rootNode);
		HAMTNode<String, String> helloWorldNode = new HAMTLeafNode<>(ImmutableEntry.of("Hello", "World"));
		HAMTNode<String, String> fooBarNode = new HAMTLeafNode<>(ImmutableEntry.of("Foo", "Bar"));
		HAMTNode<String, String> johnDoeNode = new HAMTLeafNode<>(ImmutableEntry.of("John", "Doe"));
		rootNode = rootNode.attachChild(0, helloWorldNode);
		rootNode = rootNode.attachChild(16, fooBarNode);
		rootNode = rootNode.attachChild(31, johnDoeNode);
		assertEquals(helloWorldNode, rootNode.findNextChildNode(0));
		assertEquals(fooBarNode, rootNode.findNextChildNode(16));
		assertEquals(johnDoeNode, rootNode.findNextChildNode(31));
	}

	@Test
	public void attachingAChildCreatesANewNode() {
		HAMTIndexNode<String, String> node = new HAMTIndexNode<>();
		HAMTLeafNode<String, String> child1 = new HAMTLeafNode<>("Hello", "World");
		HAMTLeafNode<String, String> child2 = new HAMTLeafNode<>("Foo", "Bar");
		HAMTIndexNode<String, String> variant1 = node.attachChild(10, child1).attachChild(20, child2);
		assertEquals(child1, variant1.findNextChildNode(10));
		assertEquals(child2, variant1.findNextChildNode(20));
		HAMTIndexNode<String, String> variant2 = node.attachChild(20, child1).attachChild(10, child2);
		assertEquals(child1, variant2.findNextChildNode(20));
		assertEquals(child2, variant2.findNextChildNode(10));
		HAMTIndexNode<String, String> variant3 = node.attachChild(0, child1).attachChild(31, child2);
		assertEquals(child1, variant3.findNextChildNode(0));
		assertEquals(child2, variant3.findNextChildNode(31));
		HAMTIndexNode<String, String> variant4 = node.attachChild(31, child1).attachChild(0, child2);
		assertEquals(child1, variant4.findNextChildNode(31));
		assertEquals(child2, variant4.findNextChildNode(0));
	}

	@Test
	public void canOverrideValueOfKey() {
		ImmutableMap<Object, Object> tree = ImmutableMaps.newHashArrayMappedTreeMap();
		tree = tree.put("Hello", "World");
		assertEquals(1, tree.size());
		tree = tree.put("Hello", "Foo");
		assertEquals(1, tree.size());
		assertEquals("Foo", tree.get("Hello"));
		tree = tree.remove("Hello", "World");
		assertEquals(1, tree.size());
		assertEquals("Foo", tree.get("Hello"));
		tree = tree.remove("Hello", "Foo");
		assertEquals(0, tree.size());
		assertTrue(tree == ImmutableMaps.newHashArrayMappedTreeMap());
	}

	@Test
	public void removingNonExistingKeyLeavesTreeUnmodified() {
		ImmutableMap<Object, Object> tree = ImmutableMaps.newHashArrayMappedTreeMap();
		tree = tree.put("Hello", "World");
		assertTrue(tree == tree.remove("Foo"));
	}

	@Test
	public void puttingSameEntryTwiceLeavesTreeUnmodified() {
		ImmutableMap<Object, Object> tree = ImmutableMaps.newHashArrayMappedTreeMap();
		tree = tree.put("Hello", "World");
		tree = tree.put("Foo", "Bar");
		assertTrue(tree == tree.put("Hello", "World"));
	}

	@Test
	public void gettingNonExistingKeyReturnsNull() {
		ImmutableMap<Object, Object> tree = ImmutableMaps.newHashArrayMappedTreeMap();
		tree = tree.put("Ea", "v1");
		assertNull(tree.get("Hello")); // a non-colliding hash code
		assertNull(tree.get("FB")); // a collding hash code
	}

	@Test
	public void canCreateEmptyTree() {
		ImmutableMap<Object, Object> tree = HashArrayMappedTree.create();
		assertNotNull(tree);
		assertEquals(0, tree.size());
		assertTrue(tree.isEmpty());
		assertFalse(tree.iterator().hasNext());
		assertTrue(tree.entrySet().isEmpty());
		assertTrue(tree.keySet().isEmpty());
		assertTrue(tree.values().isEmpty());
	}

	@Test
	public void canAddAndRemoveSingleEntry() {
		ImmutableMap<String, String> tree = HashArrayMappedTree.create();
		ImmutableMap<String, String> afterPut = tree.put("Hello", "World");
		assertNotNull(afterPut);
		assertFalse(tree == afterPut);
		assertEquals(1, afterPut.size());
		assertFalse(afterPut.isEmpty());
		assertEquals(1, Iterators.size(afterPut.iterator()));
		assertEquals(1, afterPut.keySet().size());
		assertEquals(1, afterPut.entrySet().size());
		assertEquals(1, afterPut.values().size());
		assertEquals("World", afterPut.get("Hello"));
		assertTrue(afterPut.containsKey("Hello"));
		assertTrue(afterPut.containsValue("World"));
		assertTrue(afterPut.containsEntry("Hello", "World"));
		assertFalse(afterPut.containsEntry("Hello", "Foo"));
		ImmutableMap<String, String> afterRemove = afterPut.remove("Hello");
		assertNotNull(afterRemove);
		assertEquals(0, afterRemove.size());
		assertTrue(afterRemove.isEmpty());
		assertFalse(afterRemove.iterator().hasNext());
		assertTrue(afterRemove.entrySet().isEmpty());
		assertTrue(afterRemove.keySet().isEmpty());
		assertTrue(afterRemove.values().isEmpty());
	}

	@Test
	public void canHandleNonConflictingEntries() {
		ImmutableMap<String, String> tree = HashArrayMappedTree.create();
		tree = tree.put("Hello", "World").put("Foo", "Bar").put("Answer", "42");
		assertEquals(3, tree.size());
		assertTrue(tree.containsEntry("Hello", "World"));
		assertTrue(tree.containsEntry("Foo", "Bar"));
		assertTrue(tree.containsEntry("Answer", "42"));
		assertTrue(tree.containsValue("World"));
		assertTrue(tree.containsValue("Bar"));
		assertTrue(tree.containsValue("42"));
		assertEquals(Sets.newHashSet("Hello", "Foo", "Answer"), tree.keySet());
		assertEquals(Sets.newHashSet("Hello", "Foo", "Answer"), tree.asUnmodifiableMap().keySet());
		deconstructTreeInAllPermutationsUntilEmptyAndTest(tree);
	}

	@Test
	public void canHandleTwoConflictingEntries() {
		// the two keys "Ea" and "FB" are different strings that have the same hash code
		String key1 = "Ea";
		String key2 = "FB";
		ImmutableMap<String, String> tree = HashArrayMappedTree.create();
		tree = tree.put(key1, "v1");
		tree = tree.put(key2, "v2");
		assertEquals(2, tree.size());
		assertTrue(tree.containsEntry(key1, "v1"));
		assertTrue(tree.containsEntry(key2, "v2"));
		assertFalse(tree.containsEntry(key1, "v2"));
		assertFalse(tree.containsEntry(key2, "v1"));
		deconstructTreeInAllPermutationsUntilEmptyAndTest(tree);
	}

	@Test
	public void entryIteratorTest() {
		ImmutableMap<Object, String> map = ImmutableMaps.newHashArrayMappedTreeMap();
		// create three objects with different identities but the same hash code
		Object key1 = FixedHashCodeObject.create("Foo", "0000000000000000000000000000000");
		Object key2 = FixedHashCodeObject.create("Bar", "0000000000000000000000000000000");
		Object key3 = FixedHashCodeObject.create("Baz", "0000000000000000000000000000000");
		assertEquals(0, Iterators.size(map.iterator()));
		map = map.put(key1, "v1");
		assertEquals(1, Iterators.size(map.iterator()));
		map = map.put(key2, "v2");
		assertEquals(2, Iterators.size(map.iterator()));
		map = map.put(key3, "v3");
		assertEquals(3, Iterators.size(map.iterator()));
	}

	@Test
	public void canHandleMultipleConflictingKeys() {
		ImmutableMap<Object, String> map = ImmutableMaps.newHashArrayMappedTreeMap();
		// create three objects with different identities but the same hash code
		Object key1 = FixedHashCodeObject.create("Foo", "0000000000000000000000000000000");
		Object key2 = FixedHashCodeObject.create("Bar", "0000000000000000000000000000000");
		Object key3 = FixedHashCodeObject.create("Baz", "0000000000000000000000000000000");
		map = map.put(key1, "v1");
		map = map.put(key2, "v2");
		map = map.put(key3, "v3");
		assertEquals(3, map.size());
		assertEquals(Sets.newHashSet(key1, key2, key3), map.keySet());
		assertEquals("v1", map.get(key1));
		assertEquals("v2", map.get(key2));
		assertEquals("v3", map.get(key3));
		assertEquals(3, Iterators.size(map.iterator()));
		deconstructTreeInAllPermutationsUntilEmptyAndTest(map);
	}

	private static <T> void deconstructTreeInAllPermutationsUntilEmptyAndTest(final ImmutableMap<T, ?> map) {
		Set<T> keySet = map.keySet();
		Iterator<List<T>> permutationIterator = new PermutationIterator<>(keySet);
		while (permutationIterator.hasNext()) {
			List<T> keys = permutationIterator.next();
			ChronoLogger.logTrace("Executing permutation: " + keys);
			ImmutableMap<T, ?> testTree = map;
			for (int i = 0; i < keys.size(); i++) {
				T key = keys.get(i);
				testTree = testTree.remove(key);
				assertFalse(testTree.containsKey(key));
				// check that remaining keys are still contained
				Set<T> remainingKeySet = Sets.newHashSet();
				for (int j = i + 1; j < keys.size(); j++) {
					remainingKeySet.add(keys.get(j));
				}
				assertEquals(remainingKeySet, testTree.keySet());
			}
			// at the end, the tree should be empty
			assertEquals(0, testTree.size());
			assertTrue(testTree.isEmpty());
			assertFalse(testTree.iterator().hasNext());
			assertTrue(testTree.entrySet().isEmpty());
			assertTrue(testTree.keySet().isEmpty());
			assertTrue(testTree.values().isEmpty());
			assertTrue(testTree == ImmutableMaps.newHashArrayMappedTreeMap());
		}
	}

	@Test
	public void canCreateTreeFromMap() {
		Map<String, String> hashMap = Maps.newHashMap();
		hashMap.put("Ea", "v1");
		hashMap.put("FB", "v2");
		hashMap.put("Ea1", "v3");
		hashMap.put("Ea2", "v4");
		hashMap.put("Hello", "World");
		ImmutableMap<String, String> tree = HashArrayMappedTree.create(hashMap);
		assertNotNull(tree);
		assertEquals(hashMap, tree.asUnmodifiableMap());
	}

	@Test
	public void basicInsertionTest() {
		String key1 = "dfba832a-9dd2-447b-9921-4347d42ef2d1";
		String val1 = "v1";
		String key2 = "e07ad166-0809-429a-b0ba-755b7109d7fa";
		String val2 = "v2";
		int key1HashFirst5Bits = BitFieldUtil.getBitsAndShiftRight(key1.hashCode(), 0, 5);
		int key2HashFirst5Bits = BitFieldUtil.getBitsAndShiftRight(key2.hashCode(), 0, 5);
		int key1HashSecond5Bits = BitFieldUtil.getBitsAndShiftRight(key1.hashCode(), 5, 5);
		int key2HashSecond5Bits = BitFieldUtil.getBitsAndShiftRight(key2.hashCode(), 5, 5);
		ChronoLogger.logDebug(key1 + ".hashCode() = " + BitFieldUtil.toBinary(key1.hashCode()));
		ChronoLogger.logDebug("\t1st 5 bits: " + BitFieldUtil.toBinary(key1HashFirst5Bits).substring(27, 32) + "("
				+ key1HashFirst5Bits + ")");
		ChronoLogger.logDebug("\t2nd 5 bits: " + BitFieldUtil.toBinary(key1HashSecond5Bits).substring(27, 32) + "("
				+ key1HashSecond5Bits + ")");
		ChronoLogger.logDebug(key2 + ".hashCode() = " + BitFieldUtil.toBinary(key2.hashCode()));
		ChronoLogger.logDebug("\t1st 5 bits: " + BitFieldUtil.toBinary(key2HashFirst5Bits).substring(27, 32) + "("
				+ key2HashFirst5Bits + ")");
		ChronoLogger.logDebug("\t2nd 5 bits: " + BitFieldUtil.toBinary(key2HashSecond5Bits).substring(27, 32) + "("
				+ key2HashSecond5Bits + ")");

		ImmutableMap<String, String> tree = HashArrayMappedTree.create();
		tree = tree.put(key1, val1);
		tree = tree.put(key2, val2);
		assertEquals(2, tree.size());
		assertTrue(tree.containsEntry(key1, val1));
		assertTrue(tree.containsEntry(key2, val2));
	}

	@Test
	public void basicPutAndRemoveTest() {
		String key1 = "17b26e1e-d128-429f-9680-5d839d65f18c";
		String val1 = "04d83fed-dbf4-4a3a-a445-2c74a4e081ce";
		ImmutableMap<String, String> tree = HashArrayMappedTree.create();
		tree = tree.put(key1, val1).remove(key1);
		assertNotNull(tree);
		assertEquals(0, tree.size());
		assertTrue(tree.isEmpty());
		assertFalse(tree.iterator().hasNext());
		assertTrue(tree.entrySet().isEmpty());
		assertTrue(tree.keySet().isEmpty());
		assertTrue(tree.values().isEmpty());
		assertTrue(tree == (Object) HashArrayMappedTree.create());
	}

	@Test
	public void removeFromRootTest() {
		String key1 = "17b26e1e-d128-429f-9680-5d839d65f18c";
		String val1 = "04d83fed-dbf4-4a3a-a445-2c74a4e081ce";

		ImmutableMap<String, String> tree = HashArrayMappedTree.create();
		tree = tree.put(key1, val1).remove(key1);
		assertNotNull(tree);
		assertEquals(0, tree.size());
		assertTrue(tree.isEmpty());
		assertFalse(tree.iterator().hasNext());
		assertTrue(tree.entrySet().isEmpty());
		assertTrue(tree.keySet().isEmpty());
		assertTrue(tree.values().isEmpty());
		assertTrue(tree == (Object) HashArrayMappedTree.create());
	}

	@Test
	public void removeFromBranchTest() {
		String key1 = "781360d2-38a1-408c-9156-8f4768236539";
		String val1 = "val1";
		String key2 = "1c7b7036-990f-4408-86e0-e5a40430c813";
		String val2 = "val2";
		HashArrayMappedTree<String, String> tree = HashArrayMappedTree.create();
		tree = tree.put(key1, val1).put(key2, val2);
		tree = tree.remove(key1);
		tree = tree.remove(key2);
		assertNotNull(tree);
		assertEquals(0, tree.size());
		assertTrue(tree.isEmpty());
		assertFalse(tree.iterator().hasNext());
		assertTrue(tree.entrySet().isEmpty());
		assertTrue(tree.keySet().isEmpty());
		assertTrue(tree.values().isEmpty());
		assertEquals(0, tree.height());
		assertTrue(tree == (Object) HashArrayMappedTree.create());
	}

	@Test
	public void backToBackTestWithHashMap() {
		List<String> keyList = Lists.newLinkedList();
		Map<String, String> hashMap = Maps.newHashMap();
		HashArrayMappedTree<String, String> tree = HashArrayMappedTree.create();
		int elements = 10_000;
		for (int i = 0; i < elements; i++) {
			long treeTimeNanos = 0;
			long hashMapTimeNanos = 0;
			long compareTimeNanos = 0;
			// decide if we do an insert or a remove
			if ((Math.random() < 0.25) && (keyList.isEmpty() == false)) {
				// remove an existing key
				// pick random entry from key list
				int keyListIndex = (int) (Math.random() * (keyList.size() - 1));
				String key = keyList.remove(keyListIndex);
				assertTrue(hashMap.containsKey(key));
				assertTrue(tree.containsKey(key));
				long mapTimeBefore = System.nanoTime();
				hashMap.remove(key);
				long mapTimeAfter = System.nanoTime();
				hashMapTimeNanos = mapTimeAfter - mapTimeBefore;
				// System.out.println("REM :: key = '" + key + "'");
				long treeTimeBefore = System.nanoTime();
				tree = tree.remove(key);
				long treeTimeAfter = System.nanoTime();
				treeTimeNanos = treeTimeAfter - treeTimeBefore;
			} else {
				// add a new key
				String newKey = UUID.randomUUID().toString();
				String newValue = UUID.randomUUID().toString();
				keyList.add(newKey);
				long mapTimeBefore = System.nanoTime();
				hashMap.put(newKey, newValue);
				long mapTimeAfter = System.nanoTime();
				hashMapTimeNanos = mapTimeAfter - mapTimeBefore;
				// System.out.println("PUT :: key = '" + newKey + "', val = '" + newValue + "'");
				long treeTimeBefore = System.nanoTime();
				tree = tree.put(newKey, newValue);
				long treeTimeAfter = System.nanoTime();
				treeTimeNanos = treeTimeAfter - treeTimeBefore;
			}
			// checkTreeIntegrity((HashArrayMappedTree<String, String>) tree);
			if (tree.isEmpty()) {
				if (tree.height() != 0) {
					fail("Empty tree has non-NULL root node!");
				}
				if (tree != (Object) HashArrayMappedTree.create()) {
					fail("Empty tree is not == to new tree!");
				}
			}
			long compareTimeBefore = System.nanoTime();
			// assert equality of hashmap and tree
			for (Entry<String, String> entry : hashMap.entrySet()) {
				assertTrue(tree.containsEntry(entry));
			}
			long compareTimeAfter = System.nanoTime();
			compareTimeNanos = compareTimeAfter - compareTimeBefore;
			assertEquals(hashMap, tree.asUnmodifiableMap());
			ChronoLogger.logTrace(
					"Iteration #" + i + " complete. Size: " + tree.size() + ". HashMap op time: " + hashMapTimeNanos
							+ "ns, HAMT op time: " + treeTimeNanos + "ns, compare time: " + compareTimeNanos + "ns");
		}
	}

	@Test
	public void canSerializeAndDeserializeSingleTree() {
		ImmutableMap<String, String> map = ImmutableMaps.newHashArrayMappedTreeMap();
		map = map.put("Hello", "World");
		map = map.put("Foo", "Bar");
		map = map.put("Number", "42");
		byte[] bytes = KryoManager.serialize(map);
		ImmutableMap<String, String> map2 = KryoManager.deserialize(bytes);
		assertNotNull(map2);
		assertEquals(map.entrySet(), map2.entrySet());
		for (Entry<String, String> entry : map.entrySet()) {
			assertEquals(entry.getValue(), map2.get(entry.getKey()));
		}
		for (Entry<String, String> entry : map2.entrySet()) {
			assertEquals(entry.getValue(), map.get(entry.getKey()));
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void canSerializeAndDeserializeMultipleTrees() {
		ImmutableMap<String, String> map = ImmutableMaps.newHashArrayMappedTreeMap();
		ImmutableMap<String, String> map1 = map.put("Hello", "World");
		ImmutableMap<String, String> map2 = map.put("Foo", "Bar");
		ImmutableMap<String, String> map3 = map.put("Number", "42");
		List<ImmutableMap<String, String>> allVersions = Lists.newArrayList(map, map1, map2, map3);
		byte[] bytes = KryoManager.serialize(allVersions);
		List<ImmutableMap<String, String>> allVersions2 = KryoManager.deserialize(bytes);
		assertNotNull(allVersions2);
		assertEquals(allVersions, allVersions2);
	}

	@Test
	public void canUseSpliterator() {
		int dictionarySize = 10_000;
		Map<String, String> dictionary = Maps.newHashMap();
		for (int i = 0; i < dictionarySize; i++) {
			String key = UUID.randomUUID().toString();
			String value = UUID.randomUUID().toString();
			dictionary.put(key, value);
		}
		ImmutableMap<String, String> map = ImmutableMaps.newHashArrayMappedTreeMap();
		map = map.putAll(dictionary);
		// ChronoLogger.logDebug("Insert complete.");
		System.out.println("Insert complete.");
		assertEquals(dictionarySize, map.parallelStream().count());
	}

}
