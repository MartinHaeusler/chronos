package org.chronos.chronograph.test.index;

import static org.junit.Assert.*;

import java.util.Collections;

import org.chronos.chronograph.internal.impl.index.GraphIndexingUtils;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.chronograph.test.base.ChronoGraphUnitTest;
import org.junit.Test;

import com.google.common.collect.Sets;

public class GraphIndexingUtilsTest extends ChronoGraphUnitTest {

	@Test
	public void canIndexSingleStringValue() {
		PropertyRecord property = new PropertyRecord("test", "Hello");
		assertEquals(Collections.singleton("Hello"), GraphIndexingUtils.getStringIndexValues(property));
	}

	@Test
	public void canIndexMultipleStringValues() {
		PropertyRecord property = new PropertyRecord("test", Sets.newHashSet("Hello", "world"));
		assertEquals(Sets.newHashSet("Hello", "world"), GraphIndexingUtils.getStringIndexValues(property));
	}

	@Test
	public void canIndexSingleLongValue() {
		PropertyRecord property = new PropertyRecord("test", 1234L);
		assertEquals(Sets.newHashSet(1234L), GraphIndexingUtils.getLongIndexValues(property));
	}

	@Test
	public void canIndexMultipleLongValues() {
		PropertyRecord property = new PropertyRecord("test", Sets.newHashSet(-1234L, 5678L));
		assertEquals(Sets.newHashSet(-1234L, 5678L), GraphIndexingUtils.getLongIndexValues(property));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void indexingLongValuesWillSkipStrings() {
		PropertyRecord property = new PropertyRecord("test", Sets.newHashSet(-1234L, 456L, "-3243", "34"));
		assertEquals(Sets.newHashSet(-1234L, 456L), GraphIndexingUtils.getLongIndexValues(property));
	}

	@Test
	public void canIndexSingleDoubleValue() {
		double val = 12.34;
		PropertyRecord property = new PropertyRecord("test", val);
		assertEquals(Sets.newHashSet(val), GraphIndexingUtils.getDoubleIndexValues(property));
	}

	@Test
	public void canIndexMultipleDoubleValues() {
		double val1 = -3.1415;
		double val2 = 24.36;
		PropertyRecord property = new PropertyRecord("test", Sets.newHashSet(val1, val2));
		assertEquals(Sets.newHashSet(val1, val2), GraphIndexingUtils.getDoubleIndexValues(property));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void indexingDoubleValuesWillSkipStrings() {
		PropertyRecord property = new PropertyRecord("test", Sets.newHashSet(-1234.0, 456.0, "-3243.5", "34"));
		assertEquals(Sets.newHashSet(-1234.0, 456.0), GraphIndexingUtils.getDoubleIndexValues(property));
	}

	@Test
	public void indexingLongsAlsoAcceptsIntegers() {
		PropertyRecord property = new PropertyRecord("test", 123);
		assertEquals(Sets.newHashSet(123L), GraphIndexingUtils.getLongIndexValues(property));
	}

	@Test
	public void indexingDoublesAlsoAcceptsIntegers() {
		PropertyRecord property = new PropertyRecord("test", 123);
		assertEquals(Sets.newHashSet(123.0), GraphIndexingUtils.getDoubleIndexValues(property));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void indexingLongValuesIgnoresFloatingPointValues() {
		PropertyRecord property = new PropertyRecord("test", Sets.newHashSet(123.4, 24));
		assertEquals(Sets.newHashSet(24L), GraphIndexingUtils.getLongIndexValues(property));
	}
}
