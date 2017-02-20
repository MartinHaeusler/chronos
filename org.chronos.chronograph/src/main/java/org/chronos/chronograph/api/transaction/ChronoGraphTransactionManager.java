package org.chronos.chronograph.api.transaction;

import java.util.Date;

import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.chronos.chronodb.api.exceptions.ChronoDBBranchingException;
import org.chronos.chronograph.api.structure.ChronoGraph;

/**
 * A specialized version of the Apache Gremlin {@link Transaction} manager.
 *
 * <p>
 * Classes that implement this interface offer extended capabilities for transactions that work on temporal data stores.
 *
 * <p>
 * Clients can retrieve an instance of this interface via {@link ChronoGraph#tx()}. Classes that implement this
 * interface must not be manually instantiated by clients.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoGraphTransactionManager extends Transaction {

	/**
	 * Opens a new transaction on the master branch, at the given timestamp.
	 *
	 * <p>
	 * Please note that only one transaction can be active on a thread at any given time. If a transaction is already
	 * open on this thread, this method will cause an {@link IllegalStateException}.
	 *
	 * @param timestamp
	 *            The timestamp to read at. Must not be negative. Must not be larger than the timestamp of the last
	 *            commit on the branch.
	 *
	 * @throws IllegalStateException
	 *             If there already exists an open transaction for this thread.
	 */
	public void open(long timestamp);

	/**
	 * Opens a new transaction on the master branch, at the given date.
	 *
	 * <p>
	 * Please note that only one transaction can be active on a thread at any given time. If a transaction is already
	 * open on this thread, this method will cause an {@link IllegalStateException}.
	 *
	 * @param date
	 *            The date to read at. Must not be <code>null</code>. Must not be larger after the date of the last
	 *            commit on the branch.
	 *
	 * @throws IllegalStateException
	 *             If there already exists an open transaction for this thread.
	 */
	public void open(Date date);

	/**
	 * Opens a new transaction on the given branch, reading the latest available version.
	 *
	 * <p>
	 * Please note that only one transaction can be active on a thread at any given time. If a transaction is already
	 * open on this thread, this method will cause an {@link IllegalStateException}.
	 *
	 * @param branch
	 *            The branch to open the transaction on. Must not be <code>null</code>. Must refer to an existing
	 *            branch.
	 *
	 * @throws ChronoDBBranchingException
	 *             Thrown if there is no branch with the given name.
	 * @throws IllegalStateException
	 *             If there already exists an open transaction for this thread.
	 */
	public void open(String branch);

	/**
	 * Opens a new transaction on the given branch, at the given timestamp.
	 *
	 * <p>
	 * Please note that only one transaction can be active on a thread at any given time. If a transaction is already
	 * open on this thread, this method will cause an {@link IllegalStateException}.
	 *
	 * @param branch
	 *            The branch to open the transaction on. Must not be <code>null</code>. Must refer to an existing
	 *            branch.
	 * @param timestamp
	 *            The timestamp to read at. Must not be negative. Must not be larger than the timestamp of the last
	 *            commit on the branch.
	 *
	 * @throws ChronoDBBranchingException
	 *             Thrown if there is no branch with the given name.
	 * @throws IllegalStateException
	 *             If there already exists an open transaction for this thread.
	 */
	public void open(String branch, long timestamp);

	/**
	 * Opens a new transaction on the given branch, at the given date.
	 *
	 * <p>
	 * Please note that only one transaction can be active on a thread at any given time. If a transaction is already
	 * open on this thread, this method will cause an {@link IllegalStateException}.
	 *
	 * @param branch
	 *            The branch to open the transaction on. Must not be <code>null</code>. Must refer to an existing
	 *            branch.
	 * @param date
	 *            The date to read at. Must not be <code>null</code>. Must not be larger after the date of the last
	 *            commit on the branch.
	 *
	 * @throws ChronoDBBranchingException
	 *             Thrown if there is no branch with the given name.
	 * @throws IllegalStateException
	 *             If there already exists an open transaction for this thread.
	 */
	public void open(String branch, Date date);

	/**
	 * Resets the current transaction.
	 *
	 * <p>
	 * By resetting the graph transaction, the transaction timestamp is updated to the latest available version. In
	 * other words, after calling {@link #reset()}, all read operations will retrieve the latest available version. The
	 * transaction remains open after calling this method, i.e. clients must not call {@link #open()} after calling this
	 * method.
	 *
	 * <p>
	 * The transaction will <b>not</b> switch branches during this operation. After calling this method, the working
	 * branch will still be the same as before.
	 *
	 * <p>
	 * Please note that calling {@link #reset()} will <b>clear</b> the transaction context, i.e. any uncommitted changes
	 * will be lost!
	 */
	public void reset();

	/**
	 * Resets the current transaction and jumps to the given timestamp.
	 *
	 * <p>
	 * By resetting the graph transaction, the transaction timestamp is updated to the given one. In other words, after
	 * calling {@link #reset()}, all read operations will retrieve the given version. The transaction remains open after
	 * calling this method, i.e. clients must not call {@link #open()} after calling this method.
	 *
	 * <p>
	 * The transaction will <b>not</b> switch branches during this operation. After calling this method, the working
	 * branch will still be the same as before.
	 *
	 * <p>
	 * Please note that calling {@link #reset()} will <b>clear</b> the transaction context, i.e. any uncommitted changes
	 * will be lost!
	 *
	 * @param timestamp
	 *            The timestamp to jump to. Must not be negative. Must not be larger than the timestamp of the last
	 *            commit to the branch.
	 */
	public void reset(long timestamp);

	/**
	 * Resets the current transaction and jumps to the given date.
	 *
	 * <p>
	 * By resetting the graph transaction, the transaction date is updated to the given one. In other words, after
	 * calling {@link #reset()}, all read operations will retrieve the given version. The transaction remains open after
	 * calling this method, i.e. clients must not call {@link #open()} after calling this method.
	 *
	 * <p>
	 * The transaction will <b>not</b> switch branches during this operation. After calling this method, the working
	 * branch will still be the same as before.
	 *
	 * <p>
	 * Please note that calling {@link #reset()} will <b>clear</b> the transaction context, i.e. any uncommitted changes
	 * will be lost!
	 *
	 * @param date
	 *            The date to jump to. Must not be <code>null</code>. Must not refer to a point in time after the latest
	 *            commit to the branch.
	 */
	public void reset(Date date);

	/**
	 * Resets the current transaction and jumps to the given timestamp on the given branch.
	 *
	 * <p>
	 * By resetting the graph transaction, the transaction timestamp is updated to the given one. In other words, after
	 * calling {@link #reset()}, all read operations will retrieve the given version. The transaction remains open after
	 * calling this method, i.e. clients must not call {@link #open()} after calling this method.
	 *
	 * <p>
	 * Please note that calling {@link #reset()} will <b>clear</b> the transaction context, i.e. any uncommitted changes
	 * will be lost!
	 *
	 * @param branch
	 *            The branch to jump to. Must not be <code>null</code>. Must refer to an existing branch. If the branch
	 *            does not exist, this method throws a {@link ChronoDBBranchingException} and does not modify the
	 *            transaction state.
	 * @param timestamp
	 *            The timestamp to jump to. Must not be negative. Must not be larger than the timestamp of the last
	 *            commit to the target branch.
	 *
	 * @throws ChronoDBBranchingException
	 *             Thrown if the given branch does not exist.
	 */
	public void reset(String branch, long timestamp);

	/**
	 * Resets the current transaction and jumps to the given date on the given branch.
	 *
	 * <p>
	 * By resetting the graph transaction, the transaction date is updated to the given one. In other words, after
	 * calling {@link #reset()}, all read operations will retrieve the given version. The transaction remains open after
	 * calling this method, i.e. clients must not call {@link #open()} after calling this method.
	 *
	 * <p>
	 * Please note that calling {@link #reset()} will <b>clear</b> the transaction context, i.e. any uncommitted changes
	 * will be lost!
	 *
	 *
	 * @param branch
	 *            The branch to jump to. Must not be <code>null</code>. Must refer to an existing branch. If the branch
	 *            does not exist, this method throws a {@link ChronoDBBranchingException} and does not modify the
	 *            transaction state.
	 * @param date
	 *            The date to jump to. Must not be <code>null</code>. Must not refer to a point in time after the latest
	 *            commit to the branch.
	 *
	 * @throws ChronoDBBranchingException
	 *             Thrown if the given branch does not exist.
	 */
	public void reset(String branch, Date date);

	// =====================================================================================================================
	// THREADED TX
	// =====================================================================================================================

	/**
	 * Creates a child graph instance that acts as a transaction.
	 *
	 * <p>
	 * Multiple threads may collaborate on the returned graph instance, sharing the produced graph elements. When the
	 * first {@link ChronoGraphTransactionManager#commit() commit()} or {@link ChronoGraphTransactionManager#rollback()
	 * rollback()} is invoked, the resulting graph transaction will be closed. Any element generated from that graph
	 * will no longer be accessible afterwards. The transaction is also closed when {@link #close()} is invoked on the
	 * graph.
	 *
	 * <p>
	 * <b>!!! IMPORTANT !!!</b><br>
	 * <b>WARNING:</b> Even though the graph can be shared across multiple threads (in contrast to regular thread-bound
	 * transactions), neither the graph itself nor the elements are <b>safe for concurrent access</b>!
	 *
	 * @return The child graph that acts as a thread-independent transaction. Never <code>null</code>.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public ChronoGraph createThreadedTx();

	/**
	 * Creates a child graph instance that acts as a transaction on the given timestamp.
	 *
	 * <p>
	 * Multiple threads may collaborate on the returned graph instance, sharing the produced graph elements. When the
	 * first {@link ChronoGraphTransactionManager#commit() commit()} or {@link ChronoGraphTransactionManager#rollback()
	 * rollback()} is invoked, the resulting graph transaction will be closed. Any element generated from that graph
	 * will no longer be accessible afterwards. The transaction is also closed when {@link #close()} is invoked on the
	 * graph.
	 *
	 * <p>
	 * <b>!!! IMPORTANT !!!</b><br>
	 * <b>WARNING:</b> Even though the graph can be shared across multiple threads (in contrast to regular thread-bound
	 * transactions), neither the graph itself nor the elements are <b>safe for concurrent access</b>!
	 *
	 * @param timestamp
	 *            The timestamp on which to open the transaction. Must not be negative.
	 *
	 * @return The child graph that acts as a thread-independent transaction. Never <code>null</code>.
	 */
	public ChronoGraph createThreadedTx(long timestamp);

	/**
	 * Creates a child graph instance that acts as a transaction on the given date.
	 *
	 * <p>
	 * Multiple threads may collaborate on the returned graph instance, sharing the produced graph elements. When the
	 * first {@link ChronoGraphTransactionManager#commit() commit()} or {@link ChronoGraphTransactionManager#rollback()
	 * rollback()} is invoked, the resulting graph transaction will be closed. Any element generated from that graph
	 * will no longer be accessible afterwards. The transaction is also closed when {@link #close()} is invoked on the
	 * graph.
	 *
	 * <p>
	 * <b>!!! IMPORTANT !!!</b><br>
	 * <b>WARNING:</b> Even though the graph can be shared across multiple threads (in contrast to regular thread-bound
	 * transactions), neither the graph itself nor the elements are <b>safe for concurrent access</b>!
	 *
	 * @param date
	 *            The date on which to open the transaction. Must not be <code>null</code>.
	 *
	 * @return The child graph that acts as a thread-independent transaction. Never <code>null</code>.
	 */
	public ChronoGraph createThreadedTx(Date date);

	/**
	 * Creates a child graph instance that acts as a transaction on the given branch.
	 *
	 * <p>
	 * Multiple threads may collaborate on the returned graph instance, sharing the produced graph elements. When the
	 * first {@link ChronoGraphTransactionManager#commit() commit()} or {@link ChronoGraphTransactionManager#rollback()
	 * rollback()} is invoked, the resulting graph transaction will be closed. Any element generated from that graph
	 * will no longer be accessible afterwards. The transaction is also closed when {@link #close()} is invoked on the
	 * graph.
	 *
	 * <p>
	 * <b>!!! IMPORTANT !!!</b><br>
	 * <b>WARNING:</b> Even though the graph can be shared across multiple threads (in contrast to regular thread-bound
	 * transactions), neither the graph itself nor the elements are <b>safe for concurrent access</b>!
	 *
	 * @param branchName
	 *            The name of the branch on which to open the transaction. Must not be <code>null</code>. Must refer to
	 *            an existing branch.
	 *
	 * @return The child graph that acts as a thread-independent transaction. Never <code>null</code>.
	 */
	public ChronoGraph createThreadedTx(String branchName);

	/**
	 * Creates a child graph instance that acts as a transaction on the given branch and timestamp.
	 *
	 * <p>
	 * Multiple threads may collaborate on the returned graph instance, sharing the produced graph elements. When the
	 * first {@link ChronoGraphTransactionManager#commit() commit()} or {@link ChronoGraphTransactionManager#rollback()
	 * rollback()} is invoked, the resulting graph transaction will be closed. Any element generated from that graph
	 * will no longer be accessible afterwards. The transaction is also closed when {@link #close()} is invoked on the
	 * graph.
	 *
	 * <p>
	 * <b>!!! IMPORTANT !!!</b><br>
	 * <b>WARNING:</b> Even though the graph can be shared across multiple threads (in contrast to regular thread-bound
	 * transactions), neither the graph itself nor the elements are <b>safe for concurrent access</b>!
	 *
	 * @param branchName
	 *            The name of the branch on which to open the transaction. Must not be <code>null</code>. Must refer to
	 *            an existing branch.
	 * @param timestamp
	 *            The timestamp on which to open the transaction. Must not be negative.
	 *
	 * @return The child graph that acts as a thread-independent transaction. Never <code>null</code>.
	 */
	public ChronoGraph createThreadedTx(String branchName, long timestamp);

	/**
	 * Creates a child graph instance that acts as a transaction on the given branch and date.
	 *
	 * <p>
	 * Multiple threads may collaborate on the returned graph instance, sharing the produced graph elements. When the
	 * first {@link ChronoGraphTransactionManager#commit() commit()} or {@link ChronoGraphTransactionManager#rollback()
	 * rollback()} is invoked, the resulting graph transaction will be closed. Any element generated from that graph
	 * will no longer be accessible afterwards. The transaction is also closed when {@link #close()} is invoked on the
	 * graph.
	 *
	 * <p>
	 * <b>!!! IMPORTANT !!!</b><br>
	 * <b>WARNING:</b> Even though the graph can be shared across multiple threads (in contrast to regular thread-bound
	 * transactions), neither the graph itself nor the elements are <b>safe for concurrent access</b>!
	 *
	 * @param branchName
	 *            The name of the branch on which to open the transaction. Must not be <code>null</code>. Must refer to
	 *            an existing branch.
	 * @param date
	 *            The date on which to open the transaction. Must not be <code>null</code>.
	 *
	 * @return The child graph that acts as a thread-independent transaction. Never <code>null</code>.
	 */
	public ChronoGraph createThreadedTx(String branchName, Date date);

	/**
	 *
	 *
	 */
	public void commitIncremental();

	public void commit(Object metadata);

	// =====================================================================================================================
	// CURRENT THREAD-LOCAL TX
	// =====================================================================================================================

	/**
	 * Returns the actual graph transaction that is serving the current thread.
	 *
	 * <p>
	 * For internal purposes only.
	 *
	 * @return The transaction for the current thread, or <code>null</code> if there is no graph transaction bound to
	 *         the current thread.
	 */
	public ChronoGraphTransaction getCurrentTransaction();

}
