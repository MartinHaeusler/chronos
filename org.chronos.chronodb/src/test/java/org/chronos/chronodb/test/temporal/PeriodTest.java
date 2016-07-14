package org.chronos.chronodb.test.temporal;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;

import org.chronos.chronodb.internal.api.Period;
import org.chronos.common.test.ChronosUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(UnitTest.class)
public class PeriodTest extends ChronosUnitTest {

	@Test
	public void createRangePeriodWorks() {
		Period range = Period.createRange(0, 100);
		assertNotNull(range);
		assertEquals(0, range.getLowerBound());
		assertEquals(100, range.getUpperBound());
		assertFalse(range.isEmpty());
	}

	@Test
	public void createOpenEndedRangeWorks() {
		Period range = Period.createOpenEndedRange(100);
		assertNotNull(range);
		assertEquals(100, range.getLowerBound());
		assertEquals(Long.MAX_VALUE, range.getUpperBound());
		assertFalse(range.isEmpty());
	}

	@Test
	public void createPointPeriodWorks() {
		Period point = Period.createPoint(100);
		assertNotNull(point);
		assertEquals(100, point.getLowerBound());
		assertEquals(101, point.getUpperBound());
		assertFalse(point.isEmpty());
	}

	@Test
	public void createEmptyPeriodWorks() {
		Period empty = Period.empty();
		assertNotNull(empty);
		assertTrue(empty.isEmpty());
	}

	@Test
	public void createEternalPeriodWorks() {
		Period eternal = Period.eternal();
		assertNotNull(eternal);
		assertEquals(0, eternal.getLowerBound());
		assertEquals(Long.MAX_VALUE, eternal.getUpperBound());
	}

	@Test
	public void emptyPeriodIsSingleton() {
		Period empty1 = Period.empty();
		Period empty2 = Period.empty();
		assertNotNull(empty1);
		assertNotNull(empty2);
		assertTrue(empty1 == empty2);
	}

	@Test
	public void eternalPeriodIsSingleton() {
		Period eternal1 = Period.eternal();
		Period eternal2 = Period.eternal();
		assertNotNull(eternal1);
		assertNotNull(eternal2);
		assertTrue(eternal1 == eternal2);
	}

	@Test
	public void containsTimestampWorks() {
		Period p = Period.createRange(10, 100);
		assertFalse(p.contains(0));
		assertFalse(p.contains(9));
		assertTrue(p.contains(10));
		assertTrue(p.contains(50));
		assertTrue(p.contains(99));
		assertFalse(p.contains(100));
		assertFalse(p.contains(101));
	}

	@Test
	public void lowerBoundIsInclusive() {
		Period p = Period.createRange(10, 100);
		assertFalse(p.contains(9));
		assertTrue(p.contains(10));
		assertTrue(p.contains(11));
	}

	@Test
	public void upperBoundIsExclusive() {
		Period p = Period.createRange(10, 100);
		assertTrue(p.contains(99));
		assertFalse(p.contains(100));
		assertFalse(p.contains(101));
	}

	@Test
	public void orderingWorks() {
		Period p1 = Period.createRange(1, 5);
		Period p2 = Period.createRange(5, 7);
		Period p3 = Period.createPoint(7);
		List<Period> list = Lists.newArrayList(p3, p1, p2);
		Collections.sort(list);
		assertEquals(p1, list.get(0));
		assertEquals(p2, list.get(1));
		assertEquals(p3, list.get(2));
	}

	@Test
	public void hashCodeAndEqualsAreConsistent() {
		Period p1to5 = Period.createRange(1, 5);
		Period p2to5 = Period.createRange(2, 5);
		Period p1to6 = Period.createRange(1, 6);
		assertNotEquals(p1to5, p1to6);
		assertNotEquals(p1to5, p2to5);
		Period anotherP1to5 = Period.createRange(1, 5);
		assertEquals(p1to5, anotherP1to5);
		assertEquals(p1to5.hashCode(), anotherP1to5.hashCode());
	}

	@Test
	public void overlapsWorks() {
		Period p5to10 = Period.createRange(5, 10);
		Period p1to5 = Period.createRange(1, 5);
		Period p1to6 = Period.createRange(1, 6);
		Period p1to11 = Period.createRange(1, 11);
		Period p7to11 = Period.createRange(7, 11);
		Period p10to20 = Period.createRange(10, 20);

		// there is always an overlap with the period itself
		assertTrue(p5to10.overlaps(p5to10));

		// never overlap with empty
		assertFalse(p5to10.overlaps(Period.empty()));
		assertFalse(Period.empty().contains(p5to10));
		assertFalse(Period.empty().contains(Period.empty()));

		assertFalse(p1to5.overlaps(p5to10));
		assertFalse(p5to10.overlaps(p1to5));

		assertTrue(p1to6.overlaps(p5to10));
		assertTrue(p5to10.overlaps(p1to6));

		assertTrue(p1to11.overlaps(p5to10));
		assertTrue(p5to10.overlaps(p1to11));

		assertTrue(p7to11.overlaps(p5to10));
		assertTrue(p5to10.overlaps(p7to11));

		assertFalse(p10to20.overlaps(p5to10));
		assertFalse(p5to10.overlaps(p10to20));
	}

	@Test
	public void containsPeriodWorks() {
		Period p5to10 = Period.createRange(5, 10);
		Period p5to9 = Period.createRange(5, 9);
		Period p6to10 = Period.createRange(6, 10);
		Period p6to9 = Period.createRange(6, 9);
		Period p5to11 = Period.createRange(5, 11);
		Period p4to10 = Period.createRange(4, 10);

		// a period always contains itself
		assertTrue(p5to10.contains(p5to10));

		// never contain the empty period
		assertFalse(p5to10.contains(Period.empty()));
		assertFalse(Period.empty().contains(p5to10));
		assertFalse(Period.empty().contains(Period.empty()));

		assertTrue(p5to10.contains(p5to9));
		assertFalse(p5to9.contains(p5to10));

		assertTrue(p5to10.contains(p6to10));
		assertFalse(p6to10.contains(p5to10));

		assertTrue(p5to10.contains(p6to9));
		assertFalse(p6to9.contains(p5to10));

		assertFalse(p5to10.contains(p5to11));
		assertTrue(p5to11.contains(p5to10));

		assertFalse(p5to10.contains(p4to10));
		assertTrue(p4to10.contains(p5to10));
	}

	@Test
	public void isAdjacentToWorks() {
		Period p5to10 = Period.createRange(5, 10);
		Period p1to5 = Period.createRange(1, 5);
		Period p10to20 = Period.createRange(10, 20);
		Period p1to4 = Period.createRange(1, 4);
		Period p11to20 = Period.createRange(11, 20);
		Period p1to6 = Period.createRange(1, 6);
		Period p9to20 = Period.createRange(9, 20);

		// a period is never adjacent to itself
		assertFalse(p5to10.isAdjacentTo(p5to10));

		// a period is never adjacent to an empty period
		assertFalse(p5to10.isAdjacentTo(Period.empty()));
		assertFalse(Period.empty().isAdjacentTo(p5to10));
		assertFalse(Period.empty().isAdjacentTo(Period.empty()));

		assertTrue(p5to10.isAdjacentTo(p1to5));
		assertTrue(p1to5.isAdjacentTo(p5to10));

		assertTrue(p5to10.isAdjacentTo(p10to20));
		assertTrue(p10to20.isAdjacentTo(p5to10));

		assertFalse(p5to10.isAdjacentTo(p1to4));
		assertFalse(p1to4.isAdjacentTo(p5to10));

		assertFalse(p5to10.isAdjacentTo(p11to20));
		assertFalse(p11to20.isAdjacentTo(p1to5));

		assertFalse(p5to10.isAdjacentTo(p1to6));
		assertFalse(p1to6.isAdjacentTo(p5to10));

		assertFalse(p5to10.isAdjacentTo(p9to20));
		assertFalse(p9to20.isAdjacentTo(p5to10));
	}

	@Test
	public void isBeforeWorks() {
		Period p5to10 = Period.createRange(5, 10);
		Period p1to5 = Period.createRange(1, 5);
		Period p1to11 = Period.createRange(1, 11);

		// a period is never before itself
		assertFalse(p5to10.isBefore(p5to10));

		// a period is never before an empty period
		assertFalse(p5to10.isBefore(Period.empty()));
		assertFalse(Period.empty().isBefore(p5to10));
		assertFalse(Period.empty().isBefore(Period.empty()));

		assertTrue(p1to5.isBefore(p5to10));
		assertFalse(p5to10.isBefore(p1to5));

		assertTrue(p1to11.isBefore(p5to10));
		assertFalse(p5to10.isBefore(p1to11));
	}

	@Test
	public void isAfterWorks() {
		Period p5to10 = Period.createRange(5, 10);
		Period p1to5 = Period.createRange(1, 5);
		Period p1to11 = Period.createRange(1, 11);

		// a period is never after itself
		assertFalse(p5to10.isAfter(p5to10));

		// a period is never after an empty period
		assertFalse(p5to10.isAfter(Period.empty()));
		assertFalse(Period.empty().isAfter(p5to10));
		assertFalse(Period.empty().isAfter(Period.empty()));

		assertFalse(p1to5.isAfter(p5to10));
		assertTrue(p5to10.isAfter(p1to5));

		assertFalse(p1to11.isAfter(p5to10));
		assertTrue(p5to10.isAfter(p1to11));
	}

	@Test
	public void isStrictlyBeforeWorks() {
		Period p5to10 = Period.createRange(5, 10);
		Period p1to5 = Period.createRange(1, 5);
		Period p1to11 = Period.createRange(1, 11);

		// a period is never strictly before itself
		assertFalse(p5to10.isStrictlyBefore(p5to10));

		// a period is never strictly before an empty period
		assertFalse(p5to10.isStrictlyBefore(Period.empty()));
		assertFalse(Period.empty().isStrictlyBefore(p5to10));
		assertFalse(Period.empty().isStrictlyBefore(Period.empty()));

		assertTrue(p1to5.isStrictlyBefore(p5to10));
		assertFalse(p5to10.isStrictlyBefore(p1to5));

		assertFalse(p1to11.isStrictlyBefore(p5to10));
		assertFalse(p5to10.isStrictlyBefore(p1to11));
	}

	@Test
	public void isStrictlyAfterWorks() {
		Period p5to10 = Period.createRange(5, 10);
		Period p1to5 = Period.createRange(1, 5);
		Period p1to11 = Period.createRange(1, 11);

		// a period is never strictly after itself
		assertFalse(p5to10.isStrictlyAfter(p5to10));

		// a period is never strictly after an empty period
		assertFalse(p5to10.isStrictlyAfter(Period.empty()));
		assertFalse(Period.empty().isStrictlyAfter(p5to10));
		assertFalse(Period.empty().isStrictlyAfter(Period.empty()));

		assertFalse(p1to5.isStrictlyAfter(p5to10));
		assertTrue(p5to10.isStrictlyAfter(p1to5));

		assertFalse(p1to11.isStrictlyAfter(p5to10));
		assertFalse(p5to10.isStrictlyAfter(p1to11));
	}

	@Test
	public void lengthWorks() {
		Period p1 = Period.createPoint(1);
		Period p1to5 = Period.createRange(1, 5);

		assertEquals(0, Period.empty().length());
		assertEquals(1, p1.length());
		assertEquals(4, p1to5.length());
	}

	@Test
	public void isOpenEndedWorks() {
		Period p1 = Period.createOpenEndedRange(100);
		Period p2 = Period.createPoint(100);
		Period p3 = Period.createRange(10, 20);
		Period p4 = Period.eternal();
		Period p5 = Period.empty();
		assertTrue(p1.isOpenEnded());
		assertFalse(p2.isOpenEnded());
		assertFalse(p3.isOpenEnded());
		assertTrue(p4.isOpenEnded());
		assertFalse(p5.isOpenEnded());
	}

	@Test
	public void setUpperBoundIsOnlyAllowedOnNonEmptyPeriods() {
		Period p1 = Period.empty();
		try {
			p1.setUpperBound(1);
			fail("#setUpperBound() succeeded on empty period!");
		} catch (IllegalStateException expected) {
			// pass
		}
	}

	@Test
	public void setUpperBoundWorks() {
		Period p1 = Period.createPoint(100);
		Period p2 = Period.createRange(10, 100);
		Period p3 = Period.eternal();

		// case of p1
		Period p1trim = p1.setUpperBound(150);
		assertEquals(101, p1.getUpperBound());
		assertEquals(150, p1trim.getUpperBound());
		assertFalse(p1 == p1trim);
		assertNotEquals(p1, p1trim);

		// case of p2
		Period p2trim = p2.setUpperBound(200);
		assertEquals(100, p2.getUpperBound());
		assertEquals(200, p2trim.getUpperBound());
		assertFalse(p2 == p2trim);
		assertNotEquals(p2, p2trim);

		// case of p3
		Period p3trim = p3.setUpperBound(200);
		assertEquals(Long.MAX_VALUE, p3.getUpperBound());
		assertEquals(200, p3trim.getUpperBound());
		assertFalse(p3 == p3trim);
		assertNotEquals(p3, p3trim);
	}

	@Test
	public void setUpperBoundCannotCreateEmptyPeriods() {
		Period p = Period.createRange(50, 100);
		try {
			// 49 is illegal, as 49 < 50
			p.setUpperBound(49);
			fail("Managed to create an empty period using #setUpperBound(...)!");
		} catch (IllegalArgumentException expected) {
			// pass
		}
		try {
			// 50 is also illegal because the upper bound is excluded; having a
			// period from 50 (inclusive) to 50 (exclusive) would result in an empty period
			p.setUpperBound(50);
			fail("Managed to create an empty period using #setUpperBound(...)!");
		} catch (IllegalArgumentException expected) {
			// pass
		}
	}

}
