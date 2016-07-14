package org.chronos.chronodb.api.builder.transaction;

import static com.google.common.base.Preconditions.*;

import java.util.Date;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.DuplicateVersionEliminationMode;

/**
 * This class represents a fluid builder API for the configuration and creation of {@link ChronoDBTransaction}
 * instances.
 *
 * <p>
 * This class is intended to be used in a fluent way. For example:
 *
 * <pre>
 * ChronoDB chronoDB = ...; // get some ChronoDB instance
 * ChronoDBTransaction transaction = chronoDB.txBuilder().onBranch("MyBranch").readOnly().build();
 * // ... now do something with the transaction
 * </pre>
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDBTransactionBuilder {

	/**
	 * Builds the {@link ChronoDBTransaction} based on the configuration in this builder.
	 *
	 * @return The {@link ChronoDBTransaction}. Never <code>null</code>.
	 */
	public ChronoDBTransaction build();

	/**
	 * Enables read-only mode on this transaction.
	 *
	 * <p>
	 * Transactions in read-only mode will refuse any modifying operations, such as
	 * {@link ChronoDBTransaction#put(String, Object)} or {@link ChronoDBTransaction#remove(String)}.
	 *
	 * @return The builder (<code>this</code>) for method chaining. Never <code>null</code>.
	 */
	public ChronoDBTransactionBuilder readOnly();

	/**
	 * Enables thread-safety on this transaction.
	 *
	 * <p>
	 * Please note that {@link ChronoDB} is capable of handling concurrent transactions. This setting only controls how
	 * one single transaction behaves under concurrent access. By default, one transaction is intended for one thread
	 * and thus not thread-safe. This method enables a thread-safe mode for a single transaction, which synchronizes the
	 * internal state appropriately. Do not use this unless it is absolutely required, as performance on non-thread-safe
	 * transactions is likely better than on thread-safe ones.
	 *
	 * @return The builder (<code>this</code>) for method chaining. Never <code>null</code>.
	 */
	public ChronoDBTransactionBuilder threadSafe();

	/**
	 * Sets up the transaction to read the contents of the {@link ChronoDB} instance at the given date.
	 *
	 * <p>
	 * Please note that this method writes to the same internal value as {@link #atTimestamp(long)}. They are
	 * semantically equivalent. The last write access to occur before {@link #build()} wins.
	 *
	 * @param date
	 *            The date to use for the read timestamp. Must not be <code>null</code>.
	 *
	 * @return The builder (<code>this</code>) for method chaining. Never <code>null</code>.
	 */
	public ChronoDBTransactionBuilder atDate(Date date);

	/**
	 * Sets up the transaction to read the contents of the {@link ChronoDB} instance at the given timestamp.
	 *
	 * <p>
	 * Please note that this method writes to the same internal value as {@link #atDate(Date)}. They are semantically
	 * equivalent. The last write access to occur before {@link #build()} wins.
	 *
	 * @param timestamp
	 *            The timestamp to use for read access. Must not be <code>null</code>.
	 *
	 * @return The builder (<code>this</code>) for method chaining. Never <code>null</code>.
	 */
	public ChronoDBTransactionBuilder atTimestamp(long timestamp);

	/**
	 * Sets the name of the {@link ChronoDB} branch to read in this transaction.
	 *
	 * <p>
	 * By default, this value is set to read the <code>master</code> branch (see:
	 * {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER}).
	 *
	 * @param branchName
	 *            The name of the branch to connect to. Must not be <code>null</code>. Must refer to an existing branch.
	 *
	 * @return The builder (<code>this</code>) for method chaining. Never <code>null</code>.
	 */
	public ChronoDBTransactionBuilder onBranch(String branchName);

	/**
	 * Sets the {@link Branch} to read from or write to in this transaction.
	 *
	 * <p>
	 * By default, this is set to the master branch.
	 *
	 * @param branch
	 *            The branch to connect to. Must not be <code>null</code>. Must refer to an existing branch.
	 *
	 * @return The builder (<code>this</code>) for method chaining. Never <code>null</code>.
	 */
	public default ChronoDBTransactionBuilder onBranch(final Branch branch) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		return this.onBranch(branch.getName());
	}

	/**
	 * Enables or disables blind overwrite protection on the constructed transaction.
	 *
	 * <p>
	 * By default, this value is set to the blind overwrite setting of the {@link ChronoDB} instance to which the
	 * transaction connects. This method allows to override the database instance defaults for a single transaction.
	 *
	 * @param enableBlindOverwriteProtection
	 *            Set this to <code>true</code> to enable blind overwrite protection (generally recommended; increased
	 *            protection against data corruption) or to <code>false</code> in order to disable it (faster;
	 *            recommended for batch jobs).
	 *
	 * @return The builder (<code>this</code>) for method chaining. Never <code>null</code>.
	 */
	public ChronoDBTransactionBuilder withBlindOverwriteProtection(boolean enableBlindOverwriteProtection);

	/**
	 * Enables or disables duplicate version elimination on the constructed transaction.
	 *
	 * <p>
	 * By default, this value is set to the duplicate version elimination setting of the {@link ChronoDB} instance to
	 * which the transaction connects. This method allows to override the database instance defaults for a single
	 * transaction.
	 *
	 * @param mode
	 *            The duplicate version elimination mode to use for this transaction only. Must not be <code>null</code>
	 *            .
	 *
	 * @return The builder (<code>this</code>) for method chaining. Never <code>null</code>.
	 */
	public ChronoDBTransactionBuilder withDuplicateVersionEliminationMode(DuplicateVersionEliminationMode mode);

}
