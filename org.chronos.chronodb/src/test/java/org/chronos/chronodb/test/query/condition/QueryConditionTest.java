package org.chronos.chronodb.test.query.condition;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;

import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category(UnitTest.class)
@RunWith(Parameterized.class)
public class QueryConditionTest {

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
				{ "Hello World", Condition.EQUALS, "Hello World", true, true },
				//
				{ "hello world", Condition.EQUALS, "Hello World", false, true },
				//
				{ "Hello World", Condition.EQUALS, "hello world", false, true },
				//
				{ "Hello Foo", Condition.EQUALS, "Hello World", false, false },
				//
				// NOT EQUALS
				//
				{ "Hello Foo", Condition.NOT_EQUALS, "Hello World", true, true },
				//
				{ "Hello World", Condition.NOT_EQUALS, "Hello World", false, false },
				//
				{ "Hello World", Condition.NOT_EQUALS, "hello world", true, false },
				//
				{ "hello world", Condition.NOT_EQUALS, "Hello World", true, false },
				//
				// CONTAINS
				//
				{ "Hello World", Condition.CONTAINS, "Hello", true, true },
				//
				{ "hello world", Condition.CONTAINS, "Hello", false, true },
				//
				{ "Hello World", Condition.CONTAINS, "hello", false, true },
				//
				{ "Hello Foo", Condition.CONTAINS, "World", false, false },
				//
				// NOT CONTAINS
				//
				{ "Hello World", Condition.NOT_CONTAINS, "Hello", false, false },
				//
				{ "hello world", Condition.NOT_CONTAINS, "Hello", true, false },
				//
				{ "Hello World", Condition.NOT_CONTAINS, "hello", true, false },
				//
				{ "Hello Foo", Condition.NOT_CONTAINS, "World", true, true },
				//
				// STARTS WITH
				//
				{ "Hello World", Condition.STARTS_WITH, "Hello", true, true },
				//
				{ "hello world", Condition.STARTS_WITH, "Hello", false, true },
				//
				{ "Hello World", Condition.STARTS_WITH, "hello", false, true },
				//
				{ "hello world", Condition.STARTS_WITH, "foo", false, false },
				//
				// NOT STARTS WITH
				//
				{ "Hello World", Condition.NOT_STARTS_WITH, "Hello", false, false },
				//
				{ "hello world", Condition.NOT_STARTS_WITH, "Hello", true, false },
				//
				{ "Hello World", Condition.NOT_STARTS_WITH, "hello", true, false },
				//
				{ "hello world", Condition.NOT_STARTS_WITH, "foo", true, true },
				//
				// ENDS WITH
				//
				{ "Hello World", Condition.ENDS_WITH, "World", true, true },
				//
				{ "hello world", Condition.ENDS_WITH, "World", false, true },
				//
				{ "Hello World", Condition.ENDS_WITH, "world", false, true },
				//
				{ "hello world", Condition.ENDS_WITH, "foo", false, false },
				//
				// NOT ENDS WITH
				//
				{ "Hello World", Condition.NOT_ENDS_WITH, "World", false, false },
				//
				{ "hello world", Condition.NOT_ENDS_WITH, "World", true, false },
				//
				{ "Hello World", Condition.NOT_ENDS_WITH, "world", true, false },
				//
				{ "hello world", Condition.NOT_ENDS_WITH, "foo", true, true },
				//
				// MATCHES REGEX
				//
				{ "Hello World", Condition.MATCHES_REGEX, ".*World.*", true, true },
				//
				{ "hello world", Condition.MATCHES_REGEX, ".*World.*", false, true },
				//
				{ "Hello World", Condition.MATCHES_REGEX, ".*world.*", false, true },
				//
				{ "hello world", Condition.MATCHES_REGEX, ".*foo.*", false, false },
				//
				// NOT MATCHES REGEX
				//
				{ "Hello World", Condition.NOT_MATCHES_REGEX, ".*World.*", false, false },
				//
				{ "hello world", Condition.NOT_MATCHES_REGEX, ".*World.*", true, false },
				//
				{ "Hello World", Condition.NOT_MATCHES_REGEX, ".*world.*", true, false },
				//
				{ "hello world", Condition.NOT_MATCHES_REGEX, ".*foo.*", true, true },

		});
	}

	@Parameter(0)
	public String text;
	@Parameter(1)
	public Condition condition;
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
