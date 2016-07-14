package org.chronos.chronodb.internal.api;

import java.util.List;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBFactory;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.query.QueryManager;
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;

/**
 * An interface that defines methods on {@link ChronoDB} for internal use.
 *
 * <p>
 * If you down-cast a {@link ChronoDB} to a {@link ChronoDBInternal}, you leave the area of the public API, which is
 * strongly discouraged!
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDBInternal extends ChronoDB {

	/**
	 * Returns the internal representation of the branch manager associated with this database instance.
	 *
	 * @return The internal representation of the branch manager. Never <code>null</code>.
	 */
	@Override
	public BranchManagerInternal getBranchManager();

	/**
	 * A method called by the {@link ChronoDBFactory} after initializing this new instance.
	 *
	 * <p>
	 * This method serves as a startup hook that can be used e.g. to check if any recovery operations are necessary.
	 */
	public void postConstruct();

	/**
	 * Returns the {@link QueryManager} associated with this database instance.
	 *
	 * @return The query manager. Never <code>null</code>.
	 */
	public QueryManager getQueryManager();

	/**
	 * Performs a non-exclusive operation on the database which can run in parallel with other non-exclusive jobs.
	 *
	 * <p>
	 * This method ensures that non-exclusive operations are properly blocked when an exclusive operation is taking
	 * place.
	 *
	 * <p>
	 * This variant of this method returns a value; if you don't need a return value, please see
	 * {@link #performNonExclusive(ChronoNonReturningJob)}.
	 *
	 * <p>
	 * It is strongly encouraged to use Java Lambda Expressions as arguments for this method.
	 *
	 * @param <T>
	 *            The type of object the job returns.
	 * @param job
	 *            The job to execute. Must not be <code>null</code>.
	 *
	 * @return The result of the job. May be <code>null</code>.
	 */
	public <T> T performNonExclusive(final ChronoReturningJob<T> job);

	/**
	 * Performs a non-exclusive operation on the database which can run in parallel with other non-exclusive jobs.
	 *
	 * <p>
	 * This method ensures that non-exclusive operations are properly blocked when an exclusive operation is taking
	 * place.
	 *
	 * <p>
	 * This variant of this method does not return a value; if you need a return value, please see
	 * {@link #performNonExclusive(ChronoReturningJob)}.
	 *
	 * <p>
	 * It is strongly encouraged to use Java Lambda Expressions as arguments for this method.
	 *
	 * @param job
	 *            The job to execute. Must not be <code>null</code>.
	 */
	public void performNonExclusive(final ChronoNonReturningJob job);

	/**
	 * Performs an exclusive operation on the database which prevents all other jobs from executing while it is active.
	 *
	 * <p>
	 * While this method is being executed, no other jobs (exlusive or non-exclusive) are started.
	 *
	 * <p>
	 * This variant of this method does not return a value; if you need a return value, please see
	 * {@link #performExclusive(ChronoReturningJob)}.
	 *
	 * <p>
	 * It is strongly encouraged to use Java Lambda Expressions as arguments for this method.
	 *
	 * @param job
	 *            The job to execute. Must not be <code>null</code>.
	 */
	public void performExclusive(final ChronoNonReturningJob job);

	/**
	 * Performs an exclusive operation on the database which prevents all other jobs from executing while it is active.
	 *
	 * <p>
	 * While this method is being executed, no other jobs (exlusive or non-exclusive) are started.
	 *
	 * <p>
	 * This variant of this method returns a value; if you don't need a return value, please see
	 * {@link #performExclusive(ChronoNonReturningJob)}.
	 *
	 * <p>
	 * It is strongly encouraged to use Java Lambda Expressions as arguments for this method.
	 *
	 * @param <T>
	 *            The type of object the job returns.
	 * @param job
	 *            The job to execute. Must not be <code>null</code>.
	 *
	 * @return The result of the job. May be <code>null</code>.
	 */
	public <T> T performExclusive(final ChronoReturningJob<T> job);

	/**
	 * Creates a transaction on this {@link ChronoDB} based on the given configuration.
	 *
	 * @param configuration
	 *            The configuration to use for transaction construction. Must not be <code>null</code>.
	 *
	 * @return The newly opened transaction. Never <code>null</code>.
	 */
	public ChronoDBTransaction tx(TransactionConfigurationInternal configuration);

	/**
	 * Creates an iterable stream of {@link ChronoDBEntry entries} that reside in this {@link ChronoDB} instance.
	 *
	 * <p>
	 * <b>IMPORTANT:</b> The resulting stream <b>must</b> be {@linkplain CloseableIterator#close() closed} by the
	 * caller!
	 *
	 * <p>
	 * Recommended usage pattern is with "try-with-resources":
	 *
	 * <pre>
	 * try (ChronoDBEntryStream stream = chronoDBInternal.entryStream()) {
	 * 	while (stream.hasNext()) {
	 * 		ChronoDBEntry entry = stream.next();
	 * 		// do something with the entry
	 * 	}
	 * } catch (Exception e) {
	 * 	// treat exception
	 * }
	 * </pre>
	 *
	 * @return The stream of entries. Never <code>null</code>, may be empty. Must be closed explicitly.
	 */
	public CloseableIterator<ChronoDBEntry> entryStream();

	/**
	 * Loads the given list of entries into this {@link ChronoDB} instance.
	 *
	 * <p>
	 * Please note that it is assumed that the corresponding branches already exist in the system. This method performs
	 * no locking on its own!
	 *
	 * @param entries
	 *            The entries to load. Must not be <code>null</code>, may be empty.
	 */
	public void loadEntries(List<ChronoDBEntry> entries);

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	/**
	 * A functional interface that represents a job to be executed on a {@link ChronoDB} or one of its components.
	 *
	 * <p>
	 * This variant of the interface specifies the {@link #execute()} method without return type. There is also
	 * {@link ChronoReturningJob} which features a return type on the {@link #execute()} method.
	 *
	 * <p>
	 * It is recommended to implement this interface using Java Lambda Expressions.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 */
	@FunctionalInterface
	public static interface ChronoNonReturningJob {

		/**
		 * Executes the job.
		 *
		 * <p>
		 * It is recommended to implement this method with Java Lambda Expressions.
		 *
		 * <p>
		 * Any exceptions thrown by this method are forwarded to the caller.
		 */
		public void execute();

	}

	/**
	 * A functional interface that represents a job to be executed on a {@link ChronoDB} or one of its components.
	 *
	 * <p>
	 * This variant of the interface specifies the {@link #execute()} method including a return type. There is also
	 * {@link ChronoNonReturningJob} which features an {@link #execute()} method that returns <code>void</code>.
	 *
	 * <p>
	 * It is recommended to implement this interface using Java Lambda Expressions.
	 *
	 * @param <T>
	 *            The return type of this job.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 */
	@FunctionalInterface
	public static interface ChronoReturningJob<T> {

		/**
		 * Executes the job.
		 *
		 * <p>
		 * It is recommended to implement this method with Java Lambda Expressions.
		 *
		 * <p>
		 * Any exceptions thrown by this method are forwarded to the caller.
		 *
		 * @return The result of the task. May be <code>null</code>.
		 */
		public T execute();

	}

}
