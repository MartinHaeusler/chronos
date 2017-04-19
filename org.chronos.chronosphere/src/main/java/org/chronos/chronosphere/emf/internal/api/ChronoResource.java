package org.chronos.chronosphere.emf.internal.api;

import java.util.Date;

import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.exceptions.ChronoSphereCommitException;
import org.chronos.chronosphere.api.exceptions.ResourceIsAlreadyClosedException;
import org.eclipse.emf.ecore.resource.Resource;

public interface ChronoResource extends Resource, AutoCloseable {

	/**
	 * Returns the graph transaction to which this resource is currently bound.
	 *
	 * <p>
	 * Over the lifetime of a resource, it can be bound to several different transactions (but never more than one at a
	 * time).
	 *
	 * @return The graph transaction this resource is currently bound to. Never <code>null</code>.
	 *
	 * @throws ResourceIsAlreadyClosedException
	 *             Thrown if {@link #isOpen()} returns <code>false</code> and this method was called.
	 */
	public ChronoGraphTransaction getGraphTransaction();

	/**
	 * Returns the timestamp on which the resource is currently operating.
	 *
	 * @return The current timestamp. Never negative. May be zero.
	 *
	 * @throws ResourceIsAlreadyClosedException
	 *             Thrown if {@link #isOpen()} returns <code>false</code> and this method was called.
	 */
	public default long getTimestamp() {
		return this.getGraphTransaction().getTimestamp();
	}

	/**
	 * Returns the {@link ChronoGraph} instance associated with this resource.
	 *
	 * @return The graph instance. Never <code>null</code>.
	 *
	 * @throws ResourceIsAlreadyClosedException
	 *             Thrown if {@link #isOpen()} returns <code>false</code> and this method was called.
	 */
	public default ChronoGraph getGraph() {
		return this.getGraphTransaction().getGraph();
	}

	/**
	 * Closes this resource.
	 *
	 *
	 * <p>
	 * <b>/!\ WARNING:</b> Closing a resource before saving it will roll back any changes on the resource or its
	 * attached elements!
	 *
	 * <p>
	 * If this resource is already closed, this method is a no-op and returns immediately.
	 *
	 *
	 *
	 * <p>
	 * This method overrides {@link AutoCloseable#close()} and removes the <code>throws Exception</code> declaration.
	 * This method will not throw checked exceptions.
	 */
	@Override
	public void close();

	/**
	 * Checks if this resource is still open.
	 *
	 * @return <code>true</code> if the resource is still open, or <code>false</code> if it has been closed.
	 */
	public boolean isOpen();

	/**
	 * Checks if this resource is closed.
	 *
	 * <p>
	 * This is just syntactic sugar for <code>isOpen() == false</code>.
	 *
	 * @return <code>true</code> if the resource is closed, otherwise <code>false</code>.
	 */
	public default boolean isClosed() {
		return this.isOpen() == false;
	}

	/**
	 * Rolls back all changes performed in the context of this resource.
	 *
	 * <p>
	 * After this operation returns, it will be in sync with the persistence backend, at the specified timestamp. The
	 * resource itself remains open and client code may continue using it as usual.
	 *
	 * <p>
	 * <b>/!\ WARNING:</b><br>
	 * This operation <b>cannot be undone</b> and all uncommitted changes will be <b>permanently lost!</b>
	 * 
	 * @throws ResourceIsAlreadyClosedException
	 *             Thrown if {@link #isOpen()} returns <code>false</code> and this method was called.
	 */
	public void rollback();

	/**
	 * Performs a full commit of the changes performed in the context of this resource.
	 *
	 * <p>
	 * After this operation returns, the changes will be persisted, and the timestamp of the resource will be advanced
	 * to the commit timestamp. This implies that all changes by the commit will continue to be visible, but also
	 * commits performed by other transactions will be visible.
	 *
	 * <p>
	 * Please note that each full commit creates a new revision for all changed elements. For larger processes (e.g.
	 * batch imports) which should show up as a single commit in the history, but have change sets which are too big to
	 * fit into main memory, clients may use {@link #commitIncremental()} instead.
	 *
	 * @throws ResourceIsAlreadyClosedException
	 *             Thrown if {@link #isOpen()} returns <code>false</code> and this method was called.
	 *
	 * @see #commitIncremental()
	 */
	public void commit();

	/**
	 * Performs an incremental commit on this resource.
	 *
	 * <p>
	 * Incremental commits can be used to insert large batches of data into a repository. Their advantage over normal
	 * commits is that they do not require the entire data to be contained in main memory before writing it to the
	 * storage backend.
	 *
	 * <p>
	 * Recommended usage of this method:
	 *
	 * <pre>
	 * ChronoResource resource = ...; // acquire a resource
	 * try {
	 * 	// ... do some heavy work
	 *	resource.commitIncremental();
	 * 	// ... more work...
	 *	resource.commitIncremental();
	 * 	// ... more work...
	 * 	// ... and finally, accept the changes and make them visible to others
	 *	resource.commit();
	 * } finally {
	 * 	// make absolutely sure that the incremental process is terminated in case of error
	 * 	resource.rollback();
	 * }
	 * </pre>
	 *
	 * <p>
	 * Using an incremental commit implies all of the following facts:
	 * <ul>
	 * <li>Only one incremental commit process may be active on any given {@link ChronoSphere} instance at any point in
	 * time. Attempting to have multiple incremental commit processes on the same {@link ChronoSphere} instance will
	 * result in a {@link ChronoSphereCommitException} on all processes, except for the first process.
	 * <li>While an incremental commit process is active, no regular commits on other transactions can be accepted.
	 * <li>Other transactions may continue to read data while an incremental commit process is active, but they cannot
	 * see the changes made by incremental commits.
	 * <li>An incremental commit process consists of several calls to {@link #commitIncremental()}, and is terminated
	 * either by a full-fledged {@link #commit()}, or by a {@link #rollback()}.
	 * <li>It is the responsibility of the caller to ensure that either {@link #commit()} or {@link #rollback()} are
	 * called after the initial {@link #commitIncremental()} was called. Failure to do so will result in this
	 * {@link ChronoSphere} instance no longer accepting any commits.
	 * <li>After the terminating {@link #commit()}, the changes will be visible to other transactions, provided that
	 * they use an appropriate timestamp.
	 * <li>The timestamp of all changes in an incremental commit process is the timestamp of the first
	 * {@link #commitIncremental()} invocation.
	 * <li>In contrast to regular transactions, several calls to {@link #commitIncremental()} may modify the same data.
	 * Overwrites within the same incremental commit process are <b>not</b> tracked by the versioning system, and follow
	 * the "last writer wins" principle.
	 * <li>In the history of any given key, an incremental commit process appears as a single, large commit. In
	 * particular, it is not possible to select a point in time where only parts of the incremental commit process were
	 * applied.
	 * <li>A call to {@link #commitIncremental()} does not update the "now" (head revision) timestamp. Only the
	 * terminating call to {@link #commit()} updates this timestamp.
	 * <li>The timestamp of the transaction executing the incremental commit will be increased after each call to
	 * {@link #commitIncremental()} in order to allow the transaction to read the data it has written in the incremental
	 * update. If the incremental commit process fails at any point, this timestamp will be reverted to the original
	 * timestamp of the transaction before the incremental commit process started.
	 * <li>Durability of the changes made by {@link #commitIncremental()} can only be guaranteed by {@link ChronoSphere}
	 * if the terminating call to {@link #commit()} is successful. Any other changes may be lost in case of errors.
	 * <li>If the JVM process is terminated during an incremental commit process, and the terminating call to
	 * {@link #commit()} has not yet been completed successfully, any data stored by that process will be lost and
	 * rolled back on the next startup.
	 * </ul>
	 *
	 * @throws ChronoSphereCommitException
	 *             Thrown if the commit fails. If this exception is thrown, the entire incremental commit process is
	 *             aborted, and any changes to the data made by that process will be rolled back.
	 * @throws ResourceIsAlreadyClosedException
	 *             Thrown if {@link #isOpen()} returns <code>false</code> and this method was called.
	 */
	public void commitIncremental();

	/**
	 * Returns the time machine that is bound to this resource.
	 *
	 * @return The time machine. Never <code>null</code>.
	 */
	public TimeMachine timeMachine();

	/**
	 * The {@link TimeMachine} controls the point in time at which data is read in an associated {@link ChronoResource}.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 *
	 */
	public interface TimeMachine {

		/**
		 * Performs a time jump with the attached {@link ChronoResource} to the specified timestamp.
		 *
		 * <p>
		 * Please note that the timestamp is subject to the following conditions:
		 * <ul>
		 * <li>The timestamp must never be negative.
		 * <li>The timestamp must not be greater than the timestamp of the last commit in the repository (i.e. it must
		 * not be "in the future").
		 * </ul>
		 *
		 * Other than the conditions outlined above, the timestamp can be chosen freely. In particular, it does not need
		 * to (but may) exactly match the timestamp of a past commit.
		 *
		 * @param timestamp
		 *            The timestamp to jump to. Must match the criteria outlined above.
		 * @see #jumpTo(Date)
		 */
		public void jumpTo(long timestamp);

		/**
		 * Performs a time jump with the attached {@link ChronoResource} to the specified date.
		 *
		 * <p>
		 * Please note that the date is subject to the following conditions:
		 * <ul>
		 * <li>It must not be <code>null</code>.
		 * <li>The date must not be after the timestamp of the last commit in the repository (i.e. it must not be
		 * "in the future").
		 * </ul>
		 *
		 * Other than the conditions outlined above, the date can be chosen freely. In particular, it does not need to
		 * (but may) exactly match the date of a past commit.
		 *
		 * @param date
		 *            The date to jump to. Must match the criteria outlined above.
		 */
		public void jumpTo(Date date);

		/**
		 * Performs a time jump with the attached {@link ChronoResource} to the latest commit (present).
		 *
		 * <p>
		 * Use this method to see the changes made by other participants to the repository since the creation of the
		 * attached {@link ChronoResource}.
		 */
		public void jumpToPresent();

	}
}
