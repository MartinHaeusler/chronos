package org.chronos.chronodb.test.query.parser;

import static org.junit.Assert.*;

import java.util.List;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.exceptions.ChronoDBQuerySyntaxException;
import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.internal.api.query.QueryTokenStream;
import org.chronos.chronodb.internal.impl.builder.query.StandardQueryTokenStream;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.parser.StandardQueryParser;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryOperatorElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryQueryOperator;
import org.chronos.chronodb.internal.impl.query.parser.ast.NotElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.QueryElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.WhereElement;
import org.chronos.chronodb.internal.impl.query.parser.token.AndToken;
import org.chronos.chronodb.internal.impl.query.parser.token.BeginToken;
import org.chronos.chronodb.internal.impl.query.parser.token.EndOfInputToken;
import org.chronos.chronodb.internal.impl.query.parser.token.EndToken;
import org.chronos.chronodb.internal.impl.query.parser.token.KeyspaceToken;
import org.chronos.chronodb.internal.impl.query.parser.token.NotToken;
import org.chronos.chronodb.internal.impl.query.parser.token.OrToken;
import org.chronos.chronodb.internal.impl.query.parser.token.QueryToken;
import org.chronos.chronodb.internal.impl.query.parser.token.WhereToken;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(UnitTest.class)
public class StandardQueryParserTest extends ChronoDBUnitTest {

	@Test
	public void basicQueryParsingWorks() {
		// prepare the list of tokens
		List<QueryToken> tokens = Lists.newArrayList();
		tokens.add(new KeyspaceToken(ChronoDBConstants.DEFAULT_KEYSPACE_NAME));
		tokens.add(new WhereToken("name", Condition.EQUALS, TextMatchMode.STRICT, "Hello World"));
		tokens.add(new EndOfInputToken());
		// convert the list into a stream
		QueryTokenStream stream = new StandardQueryTokenStream(tokens);
		// create the parser
		StandardQueryParser parser = new StandardQueryParser();
		// run it
		ChronoDBQuery query = null;
		try {
			query = parser.parse(stream);
		} catch (ChronoDBQuerySyntaxException ex) {
			fail("Parse failed! Exception is: " + ex);
		}
		assertNotNull(query);
		assertEquals(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, query.getKeyspace());
		assertTrue(query.getRootElement() instanceof WhereElement);
		WhereElement where = (WhereElement) query.getRootElement();
		assertEquals("name", where.getIndexName());
		assertEquals(Condition.EQUALS, where.getCondition());
		assertEquals("Hello World", where.getComparisonValue());
	}

	@Test
	public void missingKeyspaceIsRecognized() {
		// prepare the list of tokens
		List<QueryToken> tokens = Lists.newArrayList();
		tokens.add(new WhereToken("name", Condition.EQUALS, TextMatchMode.STRICT, "Hello World"));
		tokens.add(new EndOfInputToken());
		// convert the list into a stream
		QueryTokenStream stream = new StandardQueryTokenStream(tokens);
		// create the parser
		StandardQueryParser parser = new StandardQueryParser();
		// run it
		try {
			parser.parse(stream);
			fail("Parser did not fail on missing keyspace declaration!");
		} catch (ChronoDBQuerySyntaxException ex) {
			// expected
		}
	}

	@Test
	public void missingEndOfInputIsRecognized() {
		// prepare the list of tokens
		List<QueryToken> tokens = Lists.newArrayList();
		tokens.add(new KeyspaceToken(ChronoDBConstants.DEFAULT_KEYSPACE_NAME));
		tokens.add(new WhereToken("name", Condition.EQUALS, TextMatchMode.STRICT, "Hello World"));
		// convert the list into a stream
		QueryTokenStream stream = new StandardQueryTokenStream(tokens);
		// create the parser
		StandardQueryParser parser = new StandardQueryParser();
		// run it
		try {
			parser.parse(stream);
			fail("Parser did not fail on missing End-Of-Input!");
		} catch (ChronoDBQuerySyntaxException ex) {
			// expected
		}
	}

	@Test
	public void andBindsStrongerThanOr() {
		// prepare the list of tokens
		List<QueryToken> tokens = Lists.newArrayList();
		tokens.add(new KeyspaceToken(ChronoDBConstants.DEFAULT_KEYSPACE_NAME));
		tokens.add(new WhereToken("name", Condition.EQUALS, TextMatchMode.STRICT, "Hello World"));
		tokens.add(new AndToken());
		tokens.add(new WhereToken("foo", Condition.EQUALS, TextMatchMode.STRICT, "Bar"));
		tokens.add(new OrToken());
		tokens.add(new WhereToken("num", Condition.EQUALS, TextMatchMode.STRICT, "42"));
		tokens.add(new EndOfInputToken());
		// convert the list into a stream
		QueryTokenStream stream = new StandardQueryTokenStream(tokens);
		// create the parser
		StandardQueryParser parser = new StandardQueryParser();
		// run it
		ChronoDBQuery query = null;
		try {
			query = parser.parse(stream);
		} catch (ChronoDBQuerySyntaxException ex) {
			fail("Parse failed! Exception is: " + ex);
		}
		assertNotNull(query);
		System.out.println(query);
		QueryElement rootElement = query.getRootElement();
		assertTrue(rootElement instanceof BinaryOperatorElement);
		BinaryOperatorElement binaryRoot = (BinaryOperatorElement) rootElement;
		assertEquals(BinaryQueryOperator.OR, binaryRoot.getOperator());
	}

	@Test
	public void notBindsStrongerThanAnd() {
		List<QueryToken> tokens = Lists.newArrayList();
		tokens.add(new KeyspaceToken(ChronoDBConstants.DEFAULT_KEYSPACE_NAME));
		tokens.add(new NotToken());
		tokens.add(new WhereToken("name", Condition.EQUALS, TextMatchMode.STRICT, "Hello World"));
		tokens.add(new AndToken());
		tokens.add(new WhereToken("foo", Condition.EQUALS, TextMatchMode.STRICT, "Bar"));
		tokens.add(new EndOfInputToken());
		// convert the list into a stream
		QueryTokenStream stream = new StandardQueryTokenStream(tokens);
		// create the parser
		StandardQueryParser parser = new StandardQueryParser();
		// run it
		ChronoDBQuery query = null;
		try {
			query = parser.parse(stream);
		} catch (ChronoDBQuerySyntaxException ex) {
			fail("Parse failed! Exception is: " + ex);
		}
		assertNotNull(query);
		System.out.println(query);
		QueryElement rootElement = query.getRootElement();
		assertTrue(rootElement instanceof BinaryOperatorElement);
		BinaryOperatorElement binaryRoot = (BinaryOperatorElement) rootElement;
		assertEquals(BinaryQueryOperator.AND, binaryRoot.getOperator());
	}

	@Test
	public void notBindsStrongerThanOr() {
		List<QueryToken> tokens = Lists.newArrayList();
		tokens.add(new KeyspaceToken(ChronoDBConstants.DEFAULT_KEYSPACE_NAME));
		tokens.add(new NotToken());
		tokens.add(new WhereToken("name", Condition.EQUALS, TextMatchMode.STRICT, "Hello World"));
		tokens.add(new OrToken());
		tokens.add(new WhereToken("foo", Condition.EQUALS, TextMatchMode.STRICT, "Bar"));
		tokens.add(new EndOfInputToken());
		// convert the list into a stream
		QueryTokenStream stream = new StandardQueryTokenStream(tokens);
		// create the parser
		StandardQueryParser parser = new StandardQueryParser();
		// run it
		ChronoDBQuery query = null;
		try {
			query = parser.parse(stream);
		} catch (ChronoDBQuerySyntaxException ex) {
			fail("Parse failed! Exception is: " + ex);
		}
		assertNotNull(query);
		System.out.println(query);
		QueryElement rootElement = query.getRootElement();
		assertTrue(rootElement instanceof BinaryOperatorElement);
		BinaryOperatorElement binaryRoot = (BinaryOperatorElement) rootElement;
		assertEquals(BinaryQueryOperator.OR, binaryRoot.getOperator());
	}

	@Test
	public void beginEndBindsStrongerThanNot() {
		List<QueryToken> tokens = Lists.newArrayList();
		tokens.add(new KeyspaceToken(ChronoDBConstants.DEFAULT_KEYSPACE_NAME));
		tokens.add(new NotToken());
		tokens.add(new BeginToken());
		tokens.add(new WhereToken("name", Condition.EQUALS, TextMatchMode.STRICT, "Hello World"));
		tokens.add(new OrToken());
		tokens.add(new WhereToken("foo", Condition.EQUALS, TextMatchMode.STRICT, "Bar"));
		tokens.add(new EndToken());
		tokens.add(new EndOfInputToken());
		// convert the list into a stream
		QueryTokenStream stream = new StandardQueryTokenStream(tokens);
		// create the parser
		StandardQueryParser parser = new StandardQueryParser();
		// run it
		ChronoDBQuery query = null;
		try {
			query = parser.parse(stream);
		} catch (ChronoDBQuerySyntaxException ex) {
			fail("Parse failed! Exception is: " + ex);
		}
		assertNotNull(query);
		System.out.println(query);
		QueryElement rootElement = query.getRootElement();
		assertTrue(rootElement instanceof NotElement);
	}
}
