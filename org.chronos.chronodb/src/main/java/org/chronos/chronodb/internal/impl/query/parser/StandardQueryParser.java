package org.chronos.chronodb.internal.impl.query.parser;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.exceptions.ChronoDBQuerySyntaxException;
import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.internal.api.query.QueryParser;
import org.chronos.chronodb.internal.api.query.QueryTokenStream;
import org.chronos.chronodb.internal.impl.builder.query.StandardQueryTokenStream;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryOperatorElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryQueryOperator;
import org.chronos.chronodb.internal.impl.query.parser.ast.ChronoDBQueryImpl;
import org.chronos.chronodb.internal.impl.query.parser.ast.DoubleWhereElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.LongWhereElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.NotElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.QueryElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.StringWhereElement;
import org.chronos.chronodb.internal.impl.query.parser.token.AndToken;
import org.chronos.chronodb.internal.impl.query.parser.token.BeginToken;
import org.chronos.chronodb.internal.impl.query.parser.token.EndOfInputToken;
import org.chronos.chronodb.internal.impl.query.parser.token.EndToken;
import org.chronos.chronodb.internal.impl.query.parser.token.KeyspaceToken;
import org.chronos.chronodb.internal.impl.query.parser.token.NotToken;
import org.chronos.chronodb.internal.impl.query.parser.token.OrToken;
import org.chronos.chronodb.internal.impl.query.parser.token.QueryToken;
import org.chronos.chronodb.internal.impl.query.parser.token.WhereToken;
import org.chronos.chronodb.internal.impl.query.parser.token.WhereToken.DoubleWhereDetails;
import org.chronos.chronodb.internal.impl.query.parser.token.WhereToken.LongWhereDetails;
import org.chronos.chronodb.internal.impl.query.parser.token.WhereToken.StringWhereDetails;

/**
 * This is the parser for the ChronoDB query language.
 *
 * <p>
 * This class operates on a {@link StandardQueryTokenStream} that is passed to
 * the constructor. Instances of this class can be created as needed by using
 * that constructor.
 *
 * <p>
 * The grammar of this language is as follows:<br>
 * <br>
 *
 * <pre>
 *   query      : KEYSPACE expression EOI
 *
 *   expression : term
 *              | term OR term
 *
 *   term       : factor
 *              | factor AND factor
 *
 *   factor     : BEGIN exp END
 *              | WHERE
 *              | NOT factor
 *
 * </pre>
 *
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class StandardQueryParser implements QueryParser {

	/**
	 * Parses the {@link ChronoDBQuery} from the given {@link QueryTokenStream}.
	 *
	 * <p>
	 * Please note that this method may be called only once per instance.
	 *
	 * @param tokenStream
	 *            The token stream to parse. Must not be <code>null</code>. Must
	 *            not be in use by another parser.
	 *
	 * @return The parsed query. Never <code>null</code>.
	 *
	 * @throws ChronoDBQuerySyntaxException
	 *             Thrown if a parse error occurs.
	 * @throws IllegalStateException
	 *             Thrown if this method was already called on this instance.
	 */
	@Override
	public ChronoDBQuery parse(final QueryTokenStream tokenStream) throws ChronoDBQuerySyntaxException {
		checkNotNull(tokenStream, "Precondition violation - argument 'tokenStream' must not be NULL!");
		checkArgument(tokenStream.isAtStartOfInput(),
				"Precondition violation - argument 'tokenStream' is in use by another parser!");
		return this.parseQuery(tokenStream);
	}

	/**
	 * Parses the root-level <i>query</i> in the query language from the token
	 * stream.
	 *
	 * <p>
	 * The corresponding grammar rule is:<br>
	 * <br>
	 *
	 * <pre>
	 *  query : KEYSPACE expression EOI
	 * </pre>
	 *
	 * @param tokenStream
	 *            The token stream to parse. Must not be <code>null</code>. Must
	 *            not be in use by another parser.
	 *
	 * @return The parsed AST element. Never <code>null</code>.
	 * @throws ChronoDBQuerySyntaxException
	 *             Thrown if a parse error occurs.
	 */
	private ChronoDBQuery parseQuery(final QueryTokenStream tokenStream) {
		KeyspaceToken token = this.match(tokenStream, KeyspaceToken.class);
		if (token == null) {
			throw new ChronoDBQuerySyntaxException("Missing keyspace declaration at start of query!");
		}
		String keyspace = token.getKeyspace();
		QueryElement element = this.parseExpression(tokenStream);
		EndOfInputToken eoiToken = this.match(tokenStream, EndOfInputToken.class);
		if (eoiToken == null) {
			this.throwParseErrorExpected(tokenStream, "End Of Input");
		}

		ChronoDBQueryImpl query = new ChronoDBQueryImpl(keyspace, element);
		return query;
	}

	/**
	 * Parses an <i>expression</i> in the query language from the token stream.
	 *
	 * <p>
	 * The corresponding grammar rule is:<br>
	 * <br>
	 *
	 * <pre>
	 *  expression : term
	 *             | term OR expression
	 * </pre>
	 *
	 * @param tokenStream
	 *            The token stream to parse. Must not be <code>null</code>. Must
	 *            not be in use by another parser.
	 *
	 * @return The parsed AST element. Never <code>null</code>.
	 * @throws ChronoDBQuerySyntaxException
	 *             Thrown if a parse error occurs.
	 */
	private QueryElement parseExpression(final QueryTokenStream tokenStream) throws ChronoDBQuerySyntaxException {
		QueryElement term = this.parseTerm(tokenStream);
		// check if the next token is an OR
		OrToken orToken = this.match(tokenStream, OrToken.class);
		if (orToken != null) {
			// we have the OR case, parse the second term
			QueryElement secondTerm = this.parseExpression(tokenStream);
			// construct the binary operation Element
			QueryElement orOpElement = new BinaryOperatorElement(term, BinaryQueryOperator.OR, secondTerm);
			return orOpElement;
		} else {
			// we have the simple case where an expression is just a single term, return it
			return term;
		}

	}

	/**
	 * Parses a <i>term</i> in the query language from the token stream.
	 *
	 * <p>
	 * The corresponding grammar rule is:<br>
	 * <br>
	 *
	 * <pre>
	 *  term : factor
	 *       | factor AND term
	 * </pre>
	 *
	 * @param tokenStream
	 *            The token stream to parse. Must not be <code>null</code>. Must
	 *            not be in use by another parser.
	 *
	 * @return The parsed AST element. Never <code>null</code>.
	 * @throws ChronoDBQuerySyntaxException
	 *             Thrown if a parse error occurs.
	 */
	private QueryElement parseTerm(final QueryTokenStream tokenStream) throws ChronoDBQuerySyntaxException {
		QueryElement factor = this.parseFactor(tokenStream);
		// check if the next token is an AND
		AndToken andToken = this.match(tokenStream, AndToken.class);
		if (andToken != null) {
			// we have the AND case, parse the second term
			QueryElement secondFactor = this.parseTerm(tokenStream);
			// construct the binary operation element
			QueryElement andOpElement = new BinaryOperatorElement(factor, BinaryQueryOperator.AND, secondFactor);
			return andOpElement;
		} else {
			// we have the simple case where a term is just a single factor, return it
			return factor;
		}
	}

	/**
	 * Parses a <i>factor</i> in the query language from the token stream.
	 *
	 * <p>
	 * The corresponding grammar rule is:<br>
	 * <br>
	 *
	 * <pre>
	 *  factor : BEGIN exp END
	 *         | WHERE
	 *         | NOT factor
	 * </pre>
	 *
	 * @param tokenStream
	 *            The token stream to parse. Must not be <code>null</code>. Must
	 *            not be in use by another parser.
	 *
	 * @return The parsed AST element. Never <code>null</code>.
	 * @throws ChronoDBQuerySyntaxException
	 *             Thrown if a parse error occurs.
	 */
	private QueryElement parseFactor(final QueryTokenStream tokenStream) throws ChronoDBQuerySyntaxException {
		BeginToken beginToken = this.match(tokenStream, BeginToken.class);
		if (beginToken != null) {
			// match the contained expression
			QueryElement expression = this.parseExpression(tokenStream);
			// match the END
			EndToken endToken = this.match(tokenStream, EndToken.class);
			if (endToken == null) {
				this.throwParseErrorExpected(tokenStream, "end");
			}
			// return the contained expression (as factor)
			return expression;
		}
		WhereToken whereToken = this.match(tokenStream, WhereToken.class);
		if (whereToken != null) {
			// construct the AST element for the WHERE clause and return it (as factor)
			if (whereToken.isStringWhereToken()) {
				StringWhereDetails stringWhereToken = whereToken.asStringWhereToken();
				String indexName = stringWhereToken.getIndexName();
				StringCondition condition = stringWhereToken.getCondition();
				TextMatchMode matchMode = stringWhereToken.getMatchMode();
				String comparisonValue = stringWhereToken.getComparisonValue();
				QueryElement whereElement = new StringWhereElement(indexName, condition, matchMode, comparisonValue);
				return whereElement;
			} else if (whereToken.isLongWhereToken()) {
				LongWhereDetails longWhereToken = whereToken.asLongWhereToken();
				String indexName = longWhereToken.getIndexName();
				NumberCondition condition = longWhereToken.getCondition();
				long comparisonValue = longWhereToken.getComparisonValue();
				QueryElement whereElement = new LongWhereElement(indexName, condition, comparisonValue);
				return whereElement;
			} else if (whereToken.isDoubleWhereToken()) {
				DoubleWhereDetails doubleWhereToken = whereToken.asDoubleWhereToken();
				String indexName = doubleWhereToken.getIndexName();
				NumberCondition condition = doubleWhereToken.getCondition();
				double comparisonValue = doubleWhereToken.getComparisonValue();
				double equalityTolerance = doubleWhereToken.getEqualityTolerance();
				QueryElement whereElement = new DoubleWhereElement(indexName, condition, comparisonValue,
						equalityTolerance);
				return whereElement;
			} else {
				throw new IllegalStateException("Unknown details on Where token!");
			}
		}
		NotToken notToken = this.match(tokenStream, NotToken.class);
		if (notToken != null) {
			// parse the inner factor
			QueryElement factor = this.parseFactor(tokenStream);
			// wrap the factor in the NOT element
			QueryElement notElement = new NotElement(factor);
			return notElement;
		}
		// otherwise, we have a parse error...
		this.throwParseErrorExpected(tokenStream, "begin", "where", "not");
		// this code is actually unreachable, it's here to satisfy the compiler
		return null;
	}

	/**
	 * Attempts to match the next token in the stream with the given token
	 * class.
	 *
	 * <p>
	 * If the next token in the stream is an instance of the given expected
	 * token class, the token is consumed from the stream and returned.
	 * Otherwise, the token remains in the stream and this method returns
	 * <code>null</code>.
	 *
	 * @param <T>
	 *            The expected type of token which should come next in the
	 *            stream.
	 * @param tokenStream
	 *            The token stream to parse. Must not be <code>null</code>. Must
	 *            not be in use by another parser.
	 * @param expectedTokenClass
	 *            The expected type of token. Must not be <code>null</code>.
	 * @return The next token in the stream, which also matches the given class,
	 *         or <code>null</code> if the next token in the stream does not
	 *         match the class.
	 */
	@SuppressWarnings("unchecked")
	private <T extends QueryToken> T match(final QueryTokenStream tokenStream, final Class<T> expectedTokenClass) {
		checkNotNull(expectedTokenClass, "Precondition violation - argument 'expectedTokenClass' must not be NULL!");
		if (tokenStream.hasNextToken() == false) {
			// well, there is nothing to match against...
			return null;
		}
		QueryToken token = tokenStream.lookAhead();
		if (expectedTokenClass.isInstance(token)) {
			tokenStream.getToken();
			return (T) token;
		}
		return null;
	}

	/**
	 * Throws a {@link ChronoDBQuerySyntaxException} with the given message.
	 *
	 * @param message
	 *            The message for the exception. Must not be <code>null</code>.
	 */
	private void throwParseError(final String message) {
		checkNotNull(message, "Precondition violation - argument 'message' must not be NULL!");
		throw new ChronoDBQuerySyntaxException("Parse Error: " + message);
	}

	/**
	 * Throws a {@link ChronoDBQuerySyntaxException} stating that the given
	 * sequence of tokens was expected, but the next token in the stream was
	 * found.
	 *
	 * @param tokenStream
	 *            The token stream to parse. Must not be <code>null</code>. Must
	 *            not be in use by another parser.
	 *
	 * @param expected
	 *            The expected token types. Must not be <code>null</code>.
	 */
	private void throwParseErrorExpected(final QueryTokenStream tokenStream, final String... expected) {
		StringBuilder builder = new StringBuilder();
		builder.append("Expected { ");
		String separator = "";
		for (String expectedToken : expected) {
			builder.append(separator);
			separator = ", ";
			builder.append(expectedToken);
		}
		builder.append(" }, ");
		if (tokenStream.hasNextToken()) {
			builder.append("but found '" + tokenStream.lookAhead() + "'!");
		} else {
			builder.append("but the token stream has no more tokens!");
		}
		this.throwParseError(builder.toString());
	}
}
