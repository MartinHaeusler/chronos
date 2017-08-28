package org.chronos.chronodb.test.query.condition;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;

import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.common.test.ChronosUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category(UnitTest.class)
@RunWith(Parameterized.class)
public class TextualQueryConditionTest extends ChronosUnitTest {

	@Parameters(name = "\"{0}\" {1} \"{2}\" (Strict: {3}, CI: {4})")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				//
				// PARAMETER ORDER:
				// {0} : Text body to scan
				// {1} : Condition to apply
				// {2} : Text to search for in the body
				// {3} : Should produce strict (case sensitive) match
				// {4} : Should produce case-insensitive match
				//
				// EQUALS
				//
				{ "Hello World", StringCondition.EQUALS, "Hello World", true, true },
				//
				{ "hello world", StringCondition.EQUALS, "Hello World", false, true },
				//
				{ "Hello World", StringCondition.EQUALS, "hello world", false, true },
				//
				{ "Hello Foo", StringCondition.EQUALS, "Hello World", false, false },
				//
				// NOT EQUALS
				//
				{ "Hello Foo", StringCondition.NOT_EQUALS, "Hello World", true, true },
				//
				{ "Hello World", StringCondition.NOT_EQUALS, "Hello World", false, false },
				//
				{ "Hello World", StringCondition.NOT_EQUALS, "hello world", true, false },
				//
				{ "hello world", StringCondition.NOT_EQUALS, "Hello World", true, false },
				//
				// CONTAINS
				//
				{ "Hello World", StringCondition.CONTAINS, "Hello", true, true },
				//
				{ "hello world", StringCondition.CONTAINS, "Hello", false, true },
				//
				{ "Hello World", StringCondition.CONTAINS, "hello", false, true },
				//
				{ "Hello Foo", StringCondition.CONTAINS, "World", false, false },
				//
				// NOT CONTAINS
				//
				{ "Hello World", StringCondition.NOT_CONTAINS, "Hello", false, false },
				//
				{ "hello world", StringCondition.NOT_CONTAINS, "Hello", true, false },
				//
				{ "Hello World", StringCondition.NOT_CONTAINS, "hello", true, false },
				//
				{ "Hello Foo", StringCondition.NOT_CONTAINS, "World", true, true },
				//
				// STARTS WITH
				//
				{ "Hello World", StringCondition.STARTS_WITH, "Hello", true, true },
				//
				{ "hello world", StringCondition.STARTS_WITH, "Hello", false, true },
				//
				{ "Hello World", StringCondition.STARTS_WITH, "hello", false, true },
				//
				{ "hello world", StringCondition.STARTS_WITH, "foo", false, false },
				//
				// NOT STARTS WITH
				//
				{ "Hello World", StringCondition.NOT_STARTS_WITH, "Hello", false, false },
				//
				{ "hello world", StringCondition.NOT_STARTS_WITH, "Hello", true, false },
				//
				{ "Hello World", StringCondition.NOT_STARTS_WITH, "hello", true, false },
				//
				{ "hello world", StringCondition.NOT_STARTS_WITH, "foo", true, true },
				//
				// ENDS WITH
				//
				{ "Hello World", StringCondition.ENDS_WITH, "World", true, true },
				//
				{ "hello world", StringCondition.ENDS_WITH, "World", false, true },
				//
				{ "Hello World", StringCondition.ENDS_WITH, "world", false, true },
				//
				{ "hello world", StringCondition.ENDS_WITH, "foo", false, false },
				//
				// NOT ENDS WITH
				//
				{ "Hello World", StringCondition.NOT_ENDS_WITH, "World", false, false },
				//
				{ "hello world", StringCondition.NOT_ENDS_WITH, "World", true, false },
				//
				{ "Hello World", StringCondition.NOT_ENDS_WITH, "world", true, false },
				//
				{ "hello world", StringCondition.NOT_ENDS_WITH, "foo", true, true },
				//
				// MATCHES REGEX
				//
				{ "Hello World", StringCondition.MATCHES_REGEX, ".*World.*", true, true },
				//
				{ "hello world", StringCondition.MATCHES_REGEX, ".*World.*", false, true },
				//
				{ "Hello World", StringCondition.MATCHES_REGEX, ".*world.*", false, true },
				//
				{ "hello world", StringCondition.MATCHES_REGEX, ".*foo.*", false, false },
				//
				// NOT MATCHES REGEX
				//
				{ "Hello World", StringCondition.NOT_MATCHES_REGEX, ".*World.*", false, false },
				//
				{ "hello world", StringCondition.NOT_MATCHES_REGEX, ".*World.*", true, false },
				//
				{ "Hello World", StringCondition.NOT_MATCHES_REGEX, ".*world.*", true, false },
				//
				{ "hello world", StringCondition.NOT_MATCHES_REGEX, ".*foo.*", true, true },

		});
	}

	@Parameter(0)
	public String text;
	@Parameter(1)
	public StringCondition condition;
	@Parameter(2)
	public String search;
	@Parameter(3)
	public boolean matchStrict;
	@Parameter(4)
	public boolean matchCaseInsensitive;

	@Test
	public void runTest() {
		if (this.matchStrict) {
			assertTrue(this.condition.applies(this.text, this.search, TextMatchMode.STRICT));
		} else {
			assertFalse(this.condition.applies(this.text, this.search, TextMatchMode.STRICT));
		}
		if (this.matchCaseInsensitive) {
			assertTrue(this.condition.applies(this.text, this.search, TextMatchMode.CASE_INSENSITIVE));
		} else {
			assertFalse(this.condition.applies(this.text, this.search, TextMatchMode.CASE_INSENSITIVE));
		}
	}
}
