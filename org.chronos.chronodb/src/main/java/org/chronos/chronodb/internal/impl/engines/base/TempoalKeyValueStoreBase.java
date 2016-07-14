package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.builder.transaction.ChronoDBTransactionBuilder;
import org.chronos.chronodb.api.exceptions.ChronoDBTransactionException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionBranchException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionTimestampException;
import org.chronos.chronodb.internal.api.MutableTransactionConfiguration;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.api.TransactionConfigurationInternal;
import org.chronos.chronodb.internal.impl.DefaultTransactionConfiguration;

public abstract class TempoalKeyValueStoreBase implements TemporalKeyValueStore {

	// =================================================================================================================
	// OPERATION [ TX ]
	// =================================================================================================================

	@Override
	public ChronoDBTransactionBuilder txBuilder() {
		return this.getOwningDB().txBuilder().onBranch(this.getOwningBranch());
	}

	@Override
	public ChronoDBTransaction tx(final TransactionConfigurationInternal configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		return this.performNonExclusive(() -> {
			String branchName = configuration.getBranch();
			long timestamp;
			if (configuration.isTimestampNow()) {
				timestamp = this.getNow();
			} else {
				timestamp = configuration.getTimestamp();
			}
			if (timestamp > this.getNow()) {
				throw new InvalidTransactionTimestampException("Cannot open transaction at the given date or timestamp: it's after the latest commit!");
			}
			if (branchName.equals(this.getOwningBranch().getName()) == false) {
				throw new InvalidTransactionBranchException("Cannot start transaction on branch '" + this.getOwningBranch().getName() + "' when transaction configuration specifies branch '" + configuration.getBranch() + "'!");
			}
			if (configuration.isThreadSafe()) {
				return new ThreadSafeChronoDBTransaction(this, timestamp, branchName, configuration);
			} else {
				return new StandardChronoDBTransaction(this, timestamp, branchName, configuration);
			}
		});
	}

	@Override
	public ChronoDBTransaction txInternal(final String branch, final long timestamp) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		MutableTransactionConfiguration configuration = new DefaultTransactionConfiguration();
		configuration.setTimestamp(timestamp);
		configuration.setBranch(branch);
		return new StandardChronoDBTransaction(this, timestamp, branch, configuration);
	}

	// =================================================================================================================
	// ABSTRACT METHODS
	// =================================================================================================================

	/**
	 * Verification method for implementations of this class.
	 *
	 * <p>
	 * The implementing method can perform arbitrary consistency checks on the given, newly created transaction before
	 * it is passed back to application code for processing.
	 *
	 * <p>
	 * If the implementation of this method detects any conflicts, it should throw an appropriate subclass of
	 * {@link ChronoDBTransactionException}.
	 *
	 * @param tx
	 *            The transaction to verify. Must not be <code>null</code>.
	 */
	protected abstract void verifyTransaction(ChronoDBTransaction tx);

}
