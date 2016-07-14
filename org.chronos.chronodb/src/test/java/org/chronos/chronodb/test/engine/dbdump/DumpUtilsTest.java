package org.chronos.chronodb.test.engine.dbdump;

import static org.junit.Assert.*;

import java.time.DayOfWeek;
import java.util.HashMap;

import org.chronos.chronodb.internal.impl.dump.ChronoDBDumpUtil;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@Category(UnitTest.class)
public class DumpUtilsTest extends ChronoDBUnitTest {

	@Test
	public void canRecognizePrimitivesAsWellKnownTypes() {
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(false));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject((byte) 5));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject((short) 5));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject('a'));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(123));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(123L));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(123.4f));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(123.4d));
	}

	@Test
	public void canRecognizeStringAsWellKnownType() {
		assertTrue(ChronoDBDumpUtil.isWellKnownObject("Hello World!"));
	}

	@Test
	public void canRecognizeListOfPrimitiveAsWellKnownType() {
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Lists.newArrayList(true, false)));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Lists.newArrayList((byte) 1, (byte) 2)));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Lists.newArrayList((short) 1, (short) 2)));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Lists.newArrayList('a', 'b')));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Lists.newArrayList(123, 456)));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Lists.newArrayList(123L, 456L)));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Lists.newArrayList(123.5f, 456.5f)));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Lists.newArrayList(123.5d, 456.5d)));
	}

	@Test
	public void canRecognizeListOfStringsAsWellKnownType() {
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Lists.newArrayList("Hello", "World!")));
	}

	@Test
	public void canRecognizeSetOfPrimitiveAsWellKnownType() {
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Sets.newHashSet(true, false)));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Sets.newHashSet((byte) 1, (byte) 2)));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Sets.newHashSet((short) 1, (short) 2)));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Sets.newHashSet('a', 'b')));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Sets.newHashSet(123, 456)));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Sets.newHashSet(123L, 456L)));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Sets.newHashSet(123.5f, 456.5f)));
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Sets.newHashSet(123.5d, 456.5d)));
	}

	@Test
	public void canRecognizeSetOfStringsAsWellKnownType() {
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Sets.newHashSet("Hello", "World!")));
	}

	@Test
	public void canRecognizeMapOfPrimitiveAsWellKnownType() {
		HashMap<Object, Object> map = Maps.newHashMap();
		map.put("Hello", "World");
		map.put(123, 456);
		map.put('a', 'b');
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(map));
	}

	@Test
	public void canRecognizeEnumsAsWellKnownType() {
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(DayOfWeek.MONDAY));
	}

	@Test
	public void canRecognizeListOfEnumsAsWellKnownType() {
		assertTrue(ChronoDBDumpUtil.isWellKnownObject(Lists.newArrayList(DayOfWeek.MONDAY, DayOfWeek.TUESDAY)));
	}
}
