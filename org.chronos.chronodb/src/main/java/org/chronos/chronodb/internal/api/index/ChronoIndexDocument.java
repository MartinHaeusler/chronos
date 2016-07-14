package org.chronos.chronodb.internal.api.index;

import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.impl.engines.base.IndexManagerBackend;

/**
 * A {@link ChronoIndexDocument} is an abstraction across all indexing backends.
 *
 * <p>
 * This interface is essentially just a (largely immutable) collection of primitive values:
 * <ul>
 * <li>Document ID (immutable): {@link #getDocumentId()}
 * <li>Keyspace Name (immutable): {@link #getKeyspace()}
 * <li>Key (immutable): {@link #getKey()}
 * <li>Index Name (immutable): {@link #getIndexName()}
 * <li>Indexed Value (immutable): {@link #getIndexedValue()}
 * <li>"Valid From" timestamp (immutable): {@link #getValidFromTimestamp()}
 * <li>"Valid To" timestamp (mutable): {@link #getValidToTimestamp()}, {@link #setValidToTimestamp(long)}
 * </ul>
 *
 * <p>
 * Collectively, these properties define a <i>single entry</i> in an index. The entry contains all properties required
 * for a {@link ChronoIdentifier}, alongside the index name to which it belongs, and the value that was retrieved by the
 * indexer for the object in question. In order to allow for temporal indexing, a "valid from" and "valid to" timestamp
 * are used. An index document can only be considered for a transaction if:
 * <ul>
 * <li>{@link #getValidFromTimestamp()} <= {@link ChronoDBTransaction#getTimestamp()}
 * <li>{@link ChronoDBTransaction#getTimestamp()} < {@link #getValidToTimestamp()}
 * </ul>
 *
 * <p>
 * The general idea behind this validity range is that when a value of a key is changed, the "valid to" timestamp of the
 * currently valid index document is set to the transaction timestamp, and a new index document is spawned for the new
 * value, starting with "valid from" at the transaction timestamp.
 *
 * <p>
 * This class has the following implicit invariants:
 * <ul>
 * <li>The "valid from" timestamp must always be strictly smaller than "valid to".
 * <li>The "valid to" timestamp of a document that is newly created is set to {@link Long#MAX_VALUE}.
 * <li>The "valid to" timestamp may be changed, but it may only be decreased, never increased.
 * <li>The "valid to" timestamp may not be decreased to a value lower than or equal to the "now" timestamp of the
 * branch.
 * <li>At any point in time, there must be at most one document which has the same combination of branch, keyspace, key,
 * and index name. In other words, there must never be two index documents for the same qualified key and index name at
 * any point in time.
 * </ul>
 *
 * <p>
 * Instances of this class are not to be created by clients; they are created internally by the indexing API (usually by
 * an {@link IndexManagerBackend}) and should never be exposed to the public API.
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoIndexDocument {

	/**
	 * Returns the universally unique id of this document (immutable property).
	 *
	 * @return The UUID of this document as a string. Never <code>null</code>.
	 */
	public String getDocumentId();

	/**
	 * Returns the name of the index to which this document belongs (immutable property).
	 *
	 * @return The index name. Never <code>null</code>.
	 */
	public String getIndexName();

	/**
	 * Returns the name of the branch in which the value represented by this document was indexed (immutable property).
	 *
	 * @return The branch name. Never <code>null</code>.
	 */
	public String getBranch();

	/**
	 * Returns the name of the keyspace in which the value represented by this document was indexed (immutable
	 * property).
	 *
	 * @return The keyspace name. Never <code>null</code>.
	 */
	public String getKeyspace();

	/**
	 * Returns the key that references the indexed value represented by this document (immutable property).
	 *
	 * @return The key. Never <code>null</code>.
	 */
	public String getKey();

	/**
	 * Returns the indexed value represented by this document (immutable property).
	 *
	 * @return The indexed value. Never <code>null</code>.
	 */
	public String getIndexedValue();

	/**
	 * Returns the indexed value represented by this document, in lower case representation (immutable property).
	 *
	 * @return The indexed value in lower case representation. Never <code>null</code>.
	 */
	public String getIndexedValueCaseInsensitive();

	/**
	 * Returns the "valid from" timestamp. This corresponds to the timestamp at which this document was written
	 * (immutable property).
	 *
	 * <p>
	 * Any transaction which has "valid from <= timestamp < valid to" may read and must consider this document for index
	 * queries.
	 *
	 * @return The "valid from" timestamp. Never negative.
	 */
	public long getValidFromTimestamp();

	/**
	 * Returns the "valid to" timestamp (mutable property).
	 *
	 * <p>
	 * Initially, this will be set to {@link Long#MAX_VALUE} until a new value is assigned to the key, in which case
	 * this value is decremented to indicate the termination of the validity interval.
	 *
	 * <p>
	 * Any transaction which has "valid from <= timestamp < valid to" may read and must consider this document for index
	 * queries.
	 *
	 * @return The "valid to" timestamp.
	 */
	public long getValidToTimestamp();

	/**
	 * Sets the "valid to" timestamp to the given value.
	 *
	 * <p>
	 * Note that the "valid to" timestamp may only be decremented, but never incremented. It must never be decremented
	 * to a value lower than the "now" timestamp of the current branch. It must also never be lower than or equal to the
	 * "valid from" timestamp.
	 *
	 * <p>
	 * Any transaction which has "valid from <= timestamp < valid to" may read and must consider this document for index
	 * queries.
	 *
	 * @param validTo
	 *            The new "valid to" value. Subject to the constraints explained above. Must not be negative.
	 */
	public void setValidToTimestamp(long validTo);
}
