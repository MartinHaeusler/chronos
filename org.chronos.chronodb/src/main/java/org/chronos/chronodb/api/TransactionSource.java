package org.chronos.chronodb.api;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.builder.transaction.ChronoDBTransactionBuilder;
import org.chronos.chronodb.api.exceptions.InvalidTransactionBranchException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionTimestampException;

/**
 * A {@link TransactionSource} is an object capable of producing a variety of {@link ChronoDBTransaction}s.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface TransactionSource {

	/**
	 * Produces and returns a new instance of {@link ChronoDBTransaction} on the <i>master</i> branch <i>head</i>
	 * version.
	 *
	 * <p>
	 * Opening a read transaction is a cheap operation and can be executed deliberately. Also, there is no need to
	 * explicitly close or terminate a read transaction.
	 *
	 * <p>
	 * Note that transactions are in general not thread-safe and must not be shared among threads.
	 *
	 * @return The transaction. Never <code>null</code>.
	 */
	public default ChronoDBTransaction tx() {
		return this.txBuilder().build();
	}

	/**
	 * Produces and returns a new instance of {@link ChronoDBTransaction} on the <i>master</i> branch at the given
	 * timestamp.
	 *
	 * <p>
	 * Please note that implementations may choose to refuse opening a transaction on certain timestamps, e.g. when the
	 * desired timestamp is in the future. In such cases, an {@link InvalidTransactionTimestampException} is thrown.
	 *
	 * <p>
	 * Opening a transaction is a cheap operation and can be executed deliberately. Also, there is no need to explicitly
	 * close or terminate a transaction.
	 *
	 * <p>
	 * Note that transactions are in general not thread-safe and must not be shared among threads.
	 *
	 * @param timestamp
	 *            The timestamp to use. Must not be negative.
	 *
	 * @return The transaction. Never <code>null</code>.
	 *
	 * @throws InvalidTransactionTimestampException
	 *             Thrown if the transaction could not be opened due to an invalid timestamp.
	 */
	public default ChronoDBTransaction tx(final long timestamp) throws InvalidTransactionTimestampException {
		checkArgument(timestamp >= 0,
				"Precondition violation - argument 'timestamp' must be >= 0 (value: " + timestamp + ")!");
		return this.txBuilder().atTimestamp(timestamp).build();
	}

	/**
	 * Produces and returns a new instance of {@link ChronoDBTransaction} on the <i>head</i> version of the given
	 * branch.
	 *
	 * <p>
	 * Opening a transaction is a cheap operation and can be executed deliberately. Also, there is no need to explicitly
	 * close or terminate a read transaction.
	 *
	 * <p>
	 * Note that transactions are in general not thread-safe and must not be shared among threads.
	 *
	 * @param branchName
	 *            The name of the branch to start a transaction on. Must not be <code>null</code>. Must be the name of
	 *            an existing branch.
	 *
	 * @return The transaction. Never <code>null</code>.
	 *
	 * @throws InvalidTransactionBranchException
	 *             Thrown if the transaction could not be opened due to an invalid branch name.
	 */
	public default ChronoDBTransaction tx(final String branchName) throws InvalidTransactionBranchException {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		return this.txBuilder().onBranch(branchName).build();
	}

	/**
	 * Produces and returns a new instance of {@link ChronoDBTransaction} at the given timestamp on the given branch.
	 *
	 * <p>
	 * Please note that implementations may choose to refuse opening a transaction on certain timestamps, e.g. when the
	 * desired timestamp is in the future. In such cases, an {@link InvalidTransactionTimestampException} is thrown.
	 *
	 * <p>
	 * Opening a transaction is a cheap operation and can be executed deliberately. Also, there is no need to explicitly
	 * close or terminate a transaction.
	 *
	 * <p>
	 * Note that transactions are in general not thread-safe and must not be shared among threads.
	 *
	 * @param branchName
	 *            The name of the branch to start a transaction on. Must not be <code>null</code>. Must be the name of
	 *            an existing branch.
	 * @param timestamp
	 *            The timestamp to use. Must not be negative.
	 *
	 * @return The transaction. Never <code>null</code>.
	 *
	 * @throws InvalidTransactionBranchException
	 *             Thrown if the transaction could not be opened due to an invalid branch.
	 * @throws InvalidTransactionTimestampException
	 *             Thrown if the transaction could not be opened due to an invalid timestamp.
	 */
	public default ChronoDBTransaction tx(final String branchName, final long timestamp)
			throws InvalidTransactionBranchException, InvalidTransactionTimestampException {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(timestamp >= 0,
				"Precondition violation - argument 'timestamp' must be >= 0 (value: " + timestamp + ")!");
		return this.txBuilder().onBranch(branchName).atTimestamp(timestamp).build();
	}

	/**
	 * Creates a new {@link ChronoDBTransactionBuilder} for fluent transaction configuration.
	 *
	 * <p>
	 * When you have called all relevant configuration methods, use {@link ChronoDBTransactionBuilder#build()} to
	 * receive the configured {@link ChronoDBTransaction}.
	 *
	 * @return The transaction builder.
	 */
	public ChronoDBTransactionBuilder txBuilder();

}
