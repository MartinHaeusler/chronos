package org.chronos.chronodb.internal.impl.query.parser.token;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.api.query.QueryTokenStream;

/**
 * A {@link KeyspaceToken} is usually the first token in a {@link QueryTokenStream}. It specifies the name of the
 * keyspace in which the resulting query should be executed.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class KeyspaceToken implements QueryToken {

	/** The name of the keyspace in which the query should be executed. */
	private final String keyspace;

	/**
	 * Creates a new {@link KeyspaceToken} with the given keyspace.
	 *
	 * @param keyspace
	 *            The keyspace in which the query should be executed. Must not be <code>null</code>.
	 */
	public KeyspaceToken(final String keyspace) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		this.keyspace = keyspace;
	}

	/**
	 * Returns the name of the keyspace.
	 *
	 * @return The keyspace name. Never <code>null</code>.
	 */
	public String getKeyspace() {
		return this.keyspace;
	}

}
