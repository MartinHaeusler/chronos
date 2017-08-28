package org.chronos.chronodb.test.query.optimizer;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class ConditionNegationTest extends ChronoDBUnitTest {

	@Test
	public void conditionNegationWorks() {
		for (Condition condition : Condition.values()) {
			Condition negated = condition.negate();
			// every condition can be negated
			assertNotNull(negated);
			// the negation of the negation is the original
			Condition doubleNegated = negated.negate();
			assertEquals(condition, doubleNegated);
		}
	}
}
