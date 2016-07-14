package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.chronos.chronodb.api.ChangeSetEntry;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.PutOption;
import org.chronos.chronodb.api.builder.query.QueryBuilderFinalizer;
import org.chronos.chronodb.api.builder.query.QueryBuilderStarter;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitException;
import org.chronos.chronodb.api.exceptions.TransactionIsReadOnlyException;
import org.chronos.chronodb.api.exceptions.UnknownKeyspaceException;
import org.chronos.chronodb.api.exceptions.ValueTypeMismatchException;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;

import com.google.common.collect.Sets;

public class StandardChronoDBTransaction implements ChronoDBTransaction {

	protected long timestamp;
	protected final TemporalKeyValueStore tkvs;
	protected final String branchIdentifier;

	protected final Set<ChangeSetEntry> changeSet = Sets.newConcurrentHashSet();
	protected final ChronoDBTransaction.Configuration configuration;

	protected boolean incrementalCommitProcessActive = false;
	protected long incrementalCommitProcessTimestamp = -1L;

	public StandardChronoDBTransaction(final TemporalKeyValueStore tkvs, final long timestamp,
			final String branchIdentifier, final ChronoDBTransaction.Configuration configuration) {
		checkNotNull(tkvs, "Precondition violation - argument 'tkvs' must not be NULL!");
		checkArgument(timestamp >= 0,
				"Precondition violation - argument 'timestamp' must not be negative (value: " + timestamp + ")!");
		checkNotNull(branchIdentifier, "Precondition violation - argument 'branchIdentifier' must not be NULL!");
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		this.tkvs = tkvs;
		this.timestamp = timestamp;
		this.branchIdentifier = branchIdentifier;
		this.configuration = configuration;
	}

	@Override
	public long getTimestamp() {
		if (this.incrementalCommitProcessActive) {
			if (this.incrementalCommitProcessTimestamp < 0) {
				// incremental commit process first commit is active; use the normal timestamp for now
				return this.timestamp;
			} else {
				return this.incrementalCommitProcessTimestamp;
			}
		} else {
			return this.timestamp;
		}
	}

	@Override
	public String getBranchName() {
		return this.branchIdentifier;
	}

	// =================================================================================================================
	// OPERATION [ GET ]
	// =================================================================================================================

	@Override
	public <T> T get(final String key) throws ValueTypeMismatchException {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		QualifiedKey qKey = QualifiedKey.createInDefaultKeyspace(key);
		return this.getInternal(qKey);
	}

	@Override
	public <T> T get(final String keyspaceName, final String key)
			throws ValueTypeMismatchException, UnknownKeyspaceException {
		checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		QualifiedKey qKey = QualifiedKey.create(keyspaceName, key);
		return this.getInternal(qKey);
	}

	@SuppressWarnings("unchecked")
	protected <T> T getInternal(final QualifiedKey key) throws ValueTypeMismatchException, UnknownKeyspaceException {
		Object value = this.getTKVS().performGet(this, key);
		if (value == null) {
			return null;
		}
		try {
			return (T) value;
		} catch (ClassCastException e) {
			throw new ValueTypeMismatchException(
					"Value of key '" + key + "' is of unexpected class '" + value.getClass().getName() + "'!", e);
		}
	}

	// =================================================================================================================
	// OPERATION [ EXISTS ]
	// =================================================================================================================

	@Override
	public boolean exists(final String key) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		QualifiedKey qKey = QualifiedKey.createInDefaultKeyspace(key);
		return this.existsInternal(qKey);
	}

	@Override
	public boolean exists(final String keyspaceName, final String key) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		QualifiedKey qKey = QualifiedKey.create(keyspaceName, key);
		return this.existsInternal(qKey);
	}

	protected boolean existsInternal(final QualifiedKey key) {
		// TODO PERFORMANCE JDBC: implement 'exists' natively instead of 'get' (network overhead due to blob transfer)
		Object value = this.getTKVS().performGet(this, key);
		return value != null;
	}

	// =================================================================================================================
	// OPERATION [ KEY SET ]
	// =================================================================================================================

	@Override
	public Set<String> keySet() {
		return this.keySetInternal(ChronoDBConstants.DEFAULT_KEYSPACE_NAME);
	}

	@Override
	public Set<String> keySet(final String keyspaceName) {
		checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
		return this.keySetInternal(keyspaceName);
	}

	protected Set<String> keySetInternal(final String keyspaceName) {
		return this.getTKVS().performKeySet(this, keyspaceName);
	}

	// =================================================================================================================
	// OPERATION [ HISTORY ]
	// =================================================================================================================

	@Override
	public Iterator<Long> history(final String key) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		QualifiedKey qKey = QualifiedKey.createInDefaultKeyspace(key);
		return this.historyInternal(qKey);
	}

	@Override
	public Iterator<Long> history(final String keyspaceName, final String key) {
		checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		QualifiedKey qKey = QualifiedKey.create(keyspaceName, key);
		return this.historyInternal(qKey);

	}

	protected Iterator<Long> historyInternal(final QualifiedKey key) {
		return this.getTKVS().performHistory(this, key);
	}

	// =================================================================================================================
	// OPERATION [ MODIFICATIONS BETWEEN ]
	// =================================================================================================================

	@Override
	public Iterator<TemporalKey> getModificationsInKeyspaceBetween(final String keyspace,
			final long timestampLowerBound, final long timestampUpperBound) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestampLowerBound >= 0,
				"Precondition violation - argument 'timestampLowerBound' must not be negative!");
		checkArgument(timestampUpperBound >= 0,
				"Precondition violation - argument 'timestampUpperBound' must not be negative!");
		checkArgument(timestampLowerBound <= this.getTimestamp(),
				"Precondition violation - argument 'timestampLowerBound' must not exceed the transaction timestamp!");
		checkArgument(timestampUpperBound <= this.getTimestamp(),
				"Precondition violation - argument 'timestampUpperBound' must not exceed the transaction timestamp!");
		checkArgument(timestampLowerBound <= timestampUpperBound,
				"Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
		return this.getTKVS().performGetModificationsInKeyspaceBetween(this, keyspace, timestampLowerBound,
				timestampUpperBound);
	}

	// =====================================================================================================================
	// OPERATION [ GET COMMIT METADATA ]
	// =====================================================================================================================

	@Override
	public Object getCommitMetadata(final long commitTimestamp) {
		checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(commitTimestamp <= this.getTimestamp(),
				"Precondition violation - argument 'timestamp' must not be greater than the transaction timestamp!");
		return this.getTKVS().performGetCommitMetadata(this, commitTimestamp);
	}

	// =================================================================================================================
	// OPERATION [ KEYSPACES ]
	// =================================================================================================================

	@Override
	public Set<String> keyspaces() {
		return this.getTKVS().getKeyspaces(this);
	}

	// =================================================================================================================
	// QUERY METHODS
	// =================================================================================================================

	@Override
	public QueryBuilderStarter find() {
		return this.tkvs.getOwningDB().getQueryManager().createQueryBuilder(this);
	}

	@Override
	public QueryBuilderFinalizer find(final ChronoDBQuery query) {
		return this.tkvs.getOwningDB().getQueryManager().createQueryBuilderFinalizer(this, query);
	}

	// =================================================================================================================
	// TRANSACTION CONTROL
	// =================================================================================================================

	@Override
	public void commit() throws ChronoDBCommitException {
		this.commit(null);
	}

	@Override
	public void commit(final Object commitMetadata) throws ChronoDBCommitException {
		try {
			this.getTKVS().performCommit(this, commitMetadata);
			this.changeSet.clear();
			this.timestamp = this.getTKVS().getNow();
		} finally {
			// a commit ALWAYS aborts incremental commit mode, regardless of
			// whether it succeeds or not
			this.incrementalCommitProcessActive = false;
			this.incrementalCommitProcessTimestamp = -1L;
		}
	}

	@Override
	public void commitIncremental() throws ChronoDBCommitException {
		try {
			this.incrementalCommitProcessActive = true;
			long newTimestamp = this.getTKVS().performCommitIncremental(this);
			this.changeSet.clear();
			this.incrementalCommitProcessTimestamp = newTimestamp;
		} catch (ChronoDBCommitException e) {
			// abort the incremental commit.
			this.incrementalCommitProcessTimestamp = -1L;
			this.incrementalCommitProcessActive = false;
			// clear the change set
			this.changeSet.clear();
			// propagete the exception
			throw new ChronoDBCommitException("Error during incremental commit. "
					+ "Commit process was canceled, any modifications were rolled back. "
					+ "See root cause for details.", e);
		}
	}

	@Override
	public void rollback() {
		if (this.incrementalCommitProcessActive) {
			// abort the incremental commit process and perform the rollback on the underlying TKVS
			this.getTKVS().performIncrementalRollback(this);
			this.incrementalCommitProcessActive = false;
			this.incrementalCommitProcessTimestamp = -1L;
			this.changeSet.clear();
		} else {
			// this operation is simple: as the change set is held in-memory, the entire "state" of this object
			// IS the change set. By clearing the change set, we reset the entire transaction.
			this.changeSet.clear();
		}
	}

	// =================================================================================================================
	// OPERATION [ PUT ]
	// =================================================================================================================

	@Override
	public void put(final String key, final Object value) {
		checkNotNull(value, "Argument 'value' must not be NULL! Use 'remove(...)' instead to remove it from the store.");
		this.put(key, value, PutOption.NONE);
	}

	@Override
	public void put(final String key, final Object value, final PutOption... options) {
		this.assertIsReadWrite();
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(value, "Argument 'value' must not be NULL! Use 'remove(...)' instead to remove it from the store.");
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		QualifiedKey qKey = QualifiedKey.createInDefaultKeyspace(key);
		this.putInternal(qKey, value, options);
	}

	@Override
	public void put(final String keyspaceName, final String key, final Object value) {
		this.put(keyspaceName, key, value, PutOption.NONE);
	}

	@Override
	public void put(final String keyspaceName, final String key, final Object value, final PutOption... options) {
		this.assertIsReadWrite();
		checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		QualifiedKey qKey = QualifiedKey.create(keyspaceName, key);
		this.putInternal(qKey, value, options);
	}

	protected void putInternal(final QualifiedKey key, final Object value, final PutOption[] options) {
		ChangeSetEntry entry = ChangeSetEntry.createChange(key, value, options);
		this.changeSet.add(entry);
	}

	// =================================================================================================================
	// OPERATION [ REMOVE ]
	// =================================================================================================================

	@Override
	public void remove(final String key) {
		this.assertIsReadWrite();
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		QualifiedKey qKey = QualifiedKey.createInDefaultKeyspace(key);
		this.removeInternal(qKey);
	}

	@Override
	public void remove(final String keyspaceName, final String key) {
		this.assertIsReadWrite();
		checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		QualifiedKey qKey = QualifiedKey.create(keyspaceName, key);
		this.removeInternal(qKey);
	}

	protected void removeInternal(final QualifiedKey key) {
		ChangeSetEntry entry = ChangeSetEntry.createDeletion(key);
		this.changeSet.add(entry);
	}

	// =================================================================================================================
	// MISCELLANEOUS API METHODS
	// =================================================================================================================

	@Override
	public Set<ChangeSetEntry> getChangeSet() {
		return Collections.unmodifiableSet(this.changeSet);
	}

	@Override
	public ChronoDBTransaction.Configuration getConfiguration() {
		return this.configuration;
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	protected TemporalKeyValueStore getTKVS() {
		return this.tkvs;
	}

	protected void assertIsReadWrite() {
		if (this.getConfiguration().isReadOnly()) {
			throw new TransactionIsReadOnlyException(
					"This transaction is read-only. Cannot perform modification operation.");
		}
	}

}
