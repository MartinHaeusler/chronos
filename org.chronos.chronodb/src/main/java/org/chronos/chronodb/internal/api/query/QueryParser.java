package org.chronos.chronodb.internal.api.query;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.exceptions.ChronoDBQuerySyntaxException;

/**
 * A {@link QueryParser} is responsible for converting a {@link QueryTokenStream} into a {@link ChronoDBQuery} by
 * building an Abstract Syntax Tree (AST) on top of the token stream.
 *
 * <p>
 * Query parser implementations are assumed to be <b>immutalbe</b> (in the ideal case <b>stateless</b>) and therefore
 * <b>thread-safe</b>!
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface QueryParser {

	/**
	 * Parses the given {@link QueryTokenStream}.
	 *
	 * <p>
	 * All implementations of this method are assumed to be <b>thread-safe</b> by {@link ChronoDB}!
	 *
	 * @param tokenStream
	 *            The token stream to parse. Must not be <code>null</code>, and must not be in use by another parser.
	 *            The stream is consumed during the parse process and cannot be reused.
	 *
	 * @return The parsed {@link ChronoDBQuery}. Never <code>null</code>.
	 *
	 * @throws ChronoDBQuerySyntaxException
	 *             Thrown if a parse error occurs.
	 */
	public ChronoDBQuery parse(QueryTokenStream tokenStream) throws ChronoDBQuerySyntaxException;

}
