package org.chronos.chronodb.test.temporal;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.chronos.chronodb.internal.impl.temporal.InverseUnqualifiedTemporalKey;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(UnitTest.class)
public class InverseUnqualifiedTemporalKeyTest extends ChronoDBUnitTest {

	@Test
	public void serializationFormatOrderingWorks() {
		InverseUnqualifiedTemporalKey k1 = new InverseUnqualifiedTemporalKey(1, "a");
		InverseUnqualifiedTemporalKey k2 = new InverseUnqualifiedTemporalKey(10, "a");
		InverseUnqualifiedTemporalKey k3 = new InverseUnqualifiedTemporalKey(1, "aa");
		InverseUnqualifiedTemporalKey k4 = new InverseUnqualifiedTemporalKey(100, "aaa");
		InverseUnqualifiedTemporalKey k5 = new InverseUnqualifiedTemporalKey(1, "ab");
		InverseUnqualifiedTemporalKey k6 = new InverseUnqualifiedTemporalKey(10, "ab");
		InverseUnqualifiedTemporalKey k7 = new InverseUnqualifiedTemporalKey(1, "b");
		InverseUnqualifiedTemporalKey k8 = new InverseUnqualifiedTemporalKey(10, "b");

		List<InverseUnqualifiedTemporalKey> keyList = Lists.newArrayList(k1, k2, k3, k4, k5, k6, k7, k8);
		List<String> stringList = keyList.stream().map(key -> key.toSerializableFormat()).collect(Collectors.toList());
		Collections.sort(keyList);
		Collections.sort(stringList);
		System.out.println("Key List:");
		for (InverseUnqualifiedTemporalKey key : keyList) {
			System.out.println("\t" + key.toSerializableFormat());
		}
		System.out.println("String List:");
		for (String key : stringList) {
			System.out.println("\t" + key);
		}
		for (int i = 0; i < keyList.size(); i++) {
			InverseUnqualifiedTemporalKey key = keyList.get(i);
			String string = stringList.get(i);
			assertEquals(key.toSerializableFormat(), string);
		}
	}

	@Test
	public void parseFromStringWorks() {
		InverseUnqualifiedTemporalKey k1 = new InverseUnqualifiedTemporalKey(1, "a");
		InverseUnqualifiedTemporalKey k2 = new InverseUnqualifiedTemporalKey(10, "a");
		InverseUnqualifiedTemporalKey k3 = new InverseUnqualifiedTemporalKey(1, "aa");
		InverseUnqualifiedTemporalKey k4 = new InverseUnqualifiedTemporalKey(100, "aaa");
		InverseUnqualifiedTemporalKey k5 = new InverseUnqualifiedTemporalKey(1, "ab");
		InverseUnqualifiedTemporalKey k6 = new InverseUnqualifiedTemporalKey(10, "ab");
		InverseUnqualifiedTemporalKey k7 = new InverseUnqualifiedTemporalKey(1, "b");
		InverseUnqualifiedTemporalKey k8 = new InverseUnqualifiedTemporalKey(10, "b");

		List<InverseUnqualifiedTemporalKey> keyList = Lists.newArrayList(k1, k2, k3, k4, k5, k6, k7, k8);
		List<String> stringList = keyList.stream().map(key -> key.toSerializableFormat()).collect(Collectors.toList());
		for (int i = 0; i < keyList.size(); i++) {
			InverseUnqualifiedTemporalKey key = keyList.get(i);
			String string = stringList.get(i);
			assertEquals(key, InverseUnqualifiedTemporalKey.parseSerializableFormat(string));
		}
	}

	@Test
	public void emptyKeyIsSmallestInOrder() {
		InverseUnqualifiedTemporalKey k0 = InverseUnqualifiedTemporalKey.createMinInclusive(100);
		InverseUnqualifiedTemporalKey k1 = InverseUnqualifiedTemporalKey.create(100, "a");
		InverseUnqualifiedTemporalKey k2 = InverseUnqualifiedTemporalKey.create(100, "0");
		InverseUnqualifiedTemporalKey k3 = InverseUnqualifiedTemporalKey.create(100, "_");
		assertTrue(k0.compareTo(k1) < 0);
		assertTrue(k0.compareTo(k2) < 0);
		assertTrue(k0.compareTo(k3) < 0);
	}

}
