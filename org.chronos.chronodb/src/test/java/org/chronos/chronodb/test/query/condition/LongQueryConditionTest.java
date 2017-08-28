package org.chronos.chronodb.test.query.condition;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;

import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.common.test.ChronosUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.chronos.common.util.ReflectionUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category(UnitTest.class)
@RunWith(Parameterized.class)
public class LongQueryConditionTest extends ChronosUnitTest {

	// @formatter:off
	@Parameters(name = "\"{0}\" {1} \"{2}\" (Should match: {3})")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				//
				// PARAMETER ORDER:
				// {0} : Number to check
				// {1} : Condition to apply
				// {2} : Number to compare against
				// {3} : should match (true or false)

				// EQUALS
				{ 21, NumberCondition.EQUALS, 21, true },
				{ 21, NumberCondition.EQUALS, 22, false },
				{ -13, NumberCondition.EQUALS, -13, true },
				{ -13, NumberCondition.EQUALS, 13, false },
				{ -13, NumberCondition.EQUALS, -13, true },

				// NOT EQUALS
				{ 21, NumberCondition.NOT_EQUALS, 21, false },
				{ 21, NumberCondition.NOT_EQUALS, 22, true },
				{ -13, NumberCondition.NOT_EQUALS, -13, false },
				{ -13, NumberCondition.NOT_EQUALS, 13, true },
				{ -13, NumberCondition.NOT_EQUALS, -13, false },

				// LESS THAN
				{ 21, NumberCondition.LESS_THAN, 22, true },
				{ 21, NumberCondition.LESS_THAN, 42, true },
				{ 21, NumberCondition.LESS_THAN, 21, false },
				{ 21, NumberCondition.LESS_THAN, -1, false },

				// LESS THAN OR EQUAL TO
				{ 21, NumberCondition.LESS_EQUAL, 22, true },
				{ 21, NumberCondition.LESS_EQUAL, 42, true },
				{ 21, NumberCondition.LESS_EQUAL, 21, true },
				{ 21, NumberCondition.LESS_EQUAL, -1, false },

				// GREATER THAN
				{ 21, NumberCondition.GREATER_THAN, 20, true },
				{ 21, NumberCondition.GREATER_THAN, 12, true },
				{ 21, NumberCondition.GREATER_THAN, 21, false },
				{ -1, NumberCondition.GREATER_THAN, 21, false },

				// GREATER THAN OR EQUAL TO
				{ 21, NumberCondition.GREATER_EQUAL, 20, true },
				{ 21, NumberCondition.GREATER_EQUAL, 12, true },
				{ 21, NumberCondition.GREATER_EQUAL, 21, true },
				{ -1, NumberCondition.GREATER_EQUAL, 21, false },

		});
	}
	// @formatter:on

	@Parameter(0)
	public Number number;
	@Parameter(1)
	public NumberCondition condition;
	@Parameter(2)
	public Number search;
	@Parameter(3)
	public boolean shouldMatch;

	@Test
	public void runTest() {
		Long a = ReflectionUtils.asLong(this.number);
		Long b = ReflectionUtils.asLong(this.search);
		if (this.shouldMatch) {
			assertTrue(this.condition.applies(a, b));
		} else {
			assertFalse(this.condition.applies(a, b));
		}
	}
}
