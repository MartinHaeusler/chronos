package org.chronos.chronodb.test.query.condition;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.common.test.ChronosUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class DoubleQueryConditionEqualityToleranceTest extends ChronosUnitTest {

	@Test
	public void equalityToleranceWorks() {
		assertFalse(NumberCondition.EQUALS.applies(1, 1.2, 0.1));
		assertTrue(NumberCondition.EQUALS.applies(1, 1.2, 0.2));
	}

	@Test
	public void equalityToleranceWorksInPositiveAndNegativeDirection() {
		assertTrue(NumberCondition.EQUALS.applies(1, 1.2, 0.2));
		assertTrue(NumberCondition.EQUALS.applies(1.2, 1, 0.2));
	}
}
