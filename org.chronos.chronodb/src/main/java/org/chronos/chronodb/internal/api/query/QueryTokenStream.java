package org.chronos.chronodb.internal.api.query;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.chronos.chronodb.internal.impl.query.parser.token.QueryToken;

/**
 * A {@link QueryTokenStream} is a stream of {@link QueryToken}s.
 *
 * <p>
 * Such a stream is very similar to an <code>{@link Iterator}&lt;{@link QueryToken}&gt;</code>, except that it also
 * allows to {@link #lookAhead()} to the next token (without actually moving to the next token), and also allows to
 * check if it {@link #isAtStartOfInput()}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface QueryTokenStream {

	/**
	 * Returns the current token, and moves to the next token.
	 *
	 * @return The current token. Never <code>null</code>.
	 *
	 * @throws NoSuchElementException
	 *             Thrown if there is no token to return, i.e. the stream has reached its end.
	 */
	QueryToken getToken() throws NoSuchElementException;

	/**
	 * Returns the next token (i.e. the token after the one returned by {@link #getToken()}) without moving away from
	 * the current token.
	 *
	 * @return The next token. Never <code>null</code>.
	 *
	 * @throws NoSuchElementException
	 *             Thrown if there is no next token to look ahead to.
	 */
	QueryToken lookAhead() throws NoSuchElementException;

	/**
	 * Checks if this stream has a next token to return.
	 *
	 * <p>
	 * If this method returns <code>true</code>, then neither {@link #getToken()} nor {@link #lookAhead()} may throw an
	 * {@link NoSuchElementException} until after {@link #getToken()} has been called at least once.
	 *
	 * @return <code>true</code> if there is a next token to return, <code>false</code> if the stream has reached its
	 *         end and there are no further tokens.
	 */
	boolean hasNextToken();

	/**
	 * Checks if the current token is the first token of the input.
	 *
	 * <p>
	 * This method should return <code>true</code> until {@link #getToken()} has been called once, and
	 * <code>false</code> after the first call to {@link #getToken()}.
	 *
	 * @return <code>true</code> if {@link #getToken()} was never called on this instance, otherwise <code>false</code>.
	 */
	boolean isAtStartOfInput();

}