package org.chronos.chronodb.internal.api;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.locks.Lock;

import org.chronos.chronodb.internal.impl.lock.BasicLockHolder;

/**
 * A {@link Lockable} is any object that can be locked {@linkplain #lockExclusive() exclusively} or
 * {@linkplain #lockNonExclusive() non-exclusively} using the <code>try-with-resources</code> pattern.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface Lockable {

	/**
	 * Declares that a thread is about to perform an exclusive task that can not run in parallel with other tasks.
	 *
	 * <p>
	 * All invocations of this method must adhere to the following usage pattern:
	 *
	 * <pre>
	 * try (LockHolder lock = db.lockExclusive()) {
	 * 	// ... perform tasks ...
	 * }
	 * </pre>
	 *
	 * This method ensures that exclusive operations are properly blocked when another operation is taking place.
	 *
	 * @return The object representing lock ownership. The lock ownership will be released once
	 *         {@link LockHolder#close()} is invoked, which is called automatically by the try-with-resources statement.
	 */
	public LockHolder lockExclusive();

	/**
	 * Declares that a thread is about to perform a non-exclusive task that can run in parallel with other non-exclusive
	 * locking tasks.
	 *
	 * <p>
	 * All invocations of this method must adhere to the following usage pattern:
	 *
	 * <pre>
	 * try (LockHolder lock = db.lockNonExclusive()) {
	 * 	// ... perform tasks ...
	 * }
	 * </pre>
	 *
	 * This method ensures that non-exclusive operations are properly blocked when an exclusive operation is taking
	 * place.
	 *
	 * @return The object representing lock ownership. The lock ownership will be released once
	 *         {@link LockHolder#close()} is invoked, which is called automatically by the try-with-resources statement.
	 */
	public LockHolder lockNonExclusive();

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	/**
	 * A {@link LockHolder} is an object that represents ownership over some sort of {@link Lock}.
	 *
	 * <p>
	 * The basic idea here is to have a lock representatin that implements {@link AutoCloseable} and can therefore work
	 * with the <code>try-with-resources</code> statement.
	 *
	 * <p>
	 * In general, a class making use of LockHolders should offer factory methods for the locks. These factories should
	 * create a new lock holder (e.g. via {@link #createBasicLockHolderFor(Lock)}, then call {@link #acquireLock()} on
	 * it before returning it. The caller can then use the following pattern:
	 *
	 * <pre>
	 * try (LockHolder lock = theThingToBeLocked.lockFactoryMethod()) {
	 *
	 * 	// work in locked state here
	 *
	 * } // lock.close() is automatically invoked here, calling lock.release()
	 *
	 * </pre>
	 *
	 *
	 * <p>
	 * Please note that lock holder instances are always assumed to be <b>reusable</b> and <b>reentrant</b>.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 *
	 */
	public static interface LockHolder extends AutoCloseable {

		/**
		 * Creates a basic {@link LockHolder} instance that operates on the given {@link Lock}.
		 *
		 * @param lock
		 *            The lock to operate on. Must not be <code>null</code>.
		 *
		 * @return The newly created lock holder. Never <code>null</code>.
		 */
		public static LockHolder createBasicLockHolderFor(final Lock lock) {
			checkNotNull(lock, "Precondition violation - argument 'lock' must not be NULL!");
			return new BasicLockHolder(lock);
		}

		/**
		 * Releases the ownership of all locks represented by this object.
		 *
		 * <p>
		 * All invocations after the first successful call to this method will be ignored and return immediately.
		 */
		@Override
		public default void close() {
			this.releaseLock();
		}

		/**
		 * Adds 1 to the internal lock counter.
		 *
		 * <p>
		 * <b>Calling this method is usually not required.</b> It is recommended to stick to the default
		 * {@link LockHolder} pattern using <code>try-with-resources</code> statements.
		 *
		 * @see #releaseLock()
		 */
		public void acquireLock();

		/**
		 * Subtracts 1 from the internal lock counter, unlocking the lock if the counter reaches zero.
		 *
		 * <p>
		 * <b>Calling this method is usually not required.</b> It is recommended to stick to the default
		 * {@link LockHolder} pattern using <code>try-with-resources</code> statements.
		 *
		 * @see #releaseLock()
		 */
		public void releaseLock();

	}

}
