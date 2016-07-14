package org.chronos.chronodb.test.temporal;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(UnitTest.class)
public class UnqualifiedTemporalKeyTest extends ChronoDBUnitTest {

	@Test
	public void testSerializationFormatOrdering() {
		UnqualifiedTemporalKey k1 = new UnqualifiedTemporalKey("a", 1);
		UnqualifiedTemporalKey k2 = new UnqualifiedTemporalKey("a", 10);
		UnqualifiedTemporalKey k3 = new UnqualifiedTemporalKey("aa", 1);
		UnqualifiedTemporalKey k4 = new UnqualifiedTemporalKey("aaa", 100);
		UnqualifiedTemporalKey k5 = new UnqualifiedTemporalKey("ab", 1);
		UnqualifiedTemporalKey k6 = new UnqualifiedTemporalKey("ab", 10);
		UnqualifiedTemporalKey k7 = new UnqualifiedTemporalKey("b", 1);
		UnqualifiedTemporalKey k8 = new UnqualifiedTemporalKey("b", 10);

		List<UnqualifiedTemporalKey> keyList = Lists.newArrayList(k1, k2, k3, k4, k5, k6, k7, k8);
		List<String> stringList = keyList.stream().map(key -> key.toSerializableFormat()).collect(Collectors.toList());
		Collections.sort(keyList);
		Collections.sort(stringList);
		System.out.println("Key List:");
		for (UnqualifiedTemporalKey key : keyList) {
			System.out.println("\t" + key.toSerializableFormat());
		}
		System.out.println("String List:");
		for (String key : stringList) {
			System.out.println("\t" + key);
		}
		for (int i = 0; i < keyList.size(); i++) {
			UnqualifiedTemporalKey key = keyList.get(i);
			String string = stringList.get(i);
			assertEquals(key.toSerializableFormat(), string);
		}
	}

	@Test
	public void testParseFromString() {
		UnqualifiedTemporalKey k1 = new UnqualifiedTemporalKey("a", 1);
		UnqualifiedTemporalKey k2 = new UnqualifiedTemporalKey("a", 10);
		UnqualifiedTemporalKey k3 = new UnqualifiedTemporalKey("aa", 1);
		UnqualifiedTemporalKey k4 = new UnqualifiedTemporalKey("aaa", 100);
		UnqualifiedTemporalKey k5 = new UnqualifiedTemporalKey("ab", 1);
		UnqualifiedTemporalKey k6 = new UnqualifiedTemporalKey("ab", 10);
		UnqualifiedTemporalKey k7 = new UnqualifiedTemporalKey("b", 1);
		UnqualifiedTemporalKey k8 = new UnqualifiedTemporalKey("b", 10);
		List<UnqualifiedTemporalKey> keyList = Lists.newArrayList(k1, k2, k3, k4, k5, k6, k7, k8);
		List<String> stringList = keyList.stream().map(key -> key.toSerializableFormat()).collect(Collectors.toList());
		for (int i = 0; i < keyList.size(); i++) {
			UnqualifiedTemporalKey key = keyList.get(i);
			String string = stringList.get(i);
			assertEquals(key, UnqualifiedTemporalKey.parseSerializableFormat(string));
		}
	}

	@Test
	public void testSerializationFormatOrdering2() {
		// This test case originates from a real-world use case where a bug occurred.
		// That's why the keys and timestamps look so "arbitrary".
		UnqualifiedTemporalKey k1 = new UnqualifiedTemporalKey("91fa39f4-6bc9-404f-9c90-868c3dcadf7b", 1094050925208L);
		UnqualifiedTemporalKey k2 = new UnqualifiedTemporalKey("91fa39f4-6bc9-404f-9c90-868c3dcadf7b", 1030144820210L);
		UnqualifiedTemporalKey k3 = new UnqualifiedTemporalKey("91fa39f4-6bc9-404f-9c90-868c3dcadf7b", 947698459624L);

		assertTrue(k1.compareTo(k1) == 0);
		assertTrue(k1.compareTo(k2) > 0);
		assertTrue(k1.compareTo(k3) > 0);

		assertTrue(k2.compareTo(k2) == 0);
		assertTrue(k2.compareTo(k1) < 0);
		assertTrue(k2.compareTo(k3) > 0);

		assertTrue(k3.compareTo(k3) == 0);
		assertTrue(k3.compareTo(k1) < 0);
		assertTrue(k3.compareTo(k2) < 0);

		String k1String = k1.toSerializableFormat();
		String k2String = k2.toSerializableFormat();
		String k3String = k3.toSerializableFormat();

		assertTrue(k1String.compareTo(k1String) == 0);
		assertTrue(k1String.compareTo(k2String) > 0);
		assertTrue(k1String.compareTo(k3String) > 0);

		assertTrue(k2String.compareTo(k2String) == 0);
		assertTrue(k2String.compareTo(k1String) < 0);
		assertTrue(k2String.compareTo(k3String) > 0);

		assertTrue(k3String.compareTo(k3String) == 0);
		assertTrue(k3String.compareTo(k1String) < 0);
		assertTrue(k3String.compareTo(k2String) < 0);
	}

	@Test
	public void testSerializationFormatOrdering3() {
		UnqualifiedTemporalKey k1 = new UnqualifiedTemporalKey("a", 109L);
		UnqualifiedTemporalKey k2 = new UnqualifiedTemporalKey("a", 103L);
		UnqualifiedTemporalKey k3 = new UnqualifiedTemporalKey("a", 94L);

		assertTrue(k1.compareTo(k1) == 0);
		assertTrue(k1.compareTo(k2) > 0);
		assertTrue(k1.compareTo(k3) > 0);

		assertTrue(k2.compareTo(k2) == 0);
		assertTrue(k2.compareTo(k1) < 0);
		assertTrue(k2.compareTo(k3) > 0);

		assertTrue(k3.compareTo(k3) == 0);
		assertTrue(k3.compareTo(k1) < 0);
		assertTrue(k3.compareTo(k2) < 0);

		String k1String = k1.toSerializableFormat();
		String k2String = k2.toSerializableFormat();
		String k3String = k3.toSerializableFormat();

		assertTrue(k1String.compareTo(k1String) == 0);
		assertTrue(k1String.compareTo(k2String) > 0);
		assertTrue(k1String.compareTo(k3String) > 0);

		assertTrue(k2String.compareTo(k2String) == 0);
		assertTrue(k2String.compareTo(k1String) < 0);
		assertTrue(k2String.compareTo(k3String) > 0);

		assertTrue(k3String.compareTo(k3String) == 0);
		assertTrue(k3String.compareTo(k1String) < 0);
		assertTrue(k3String.compareTo(k2String) < 0);
	}

}
