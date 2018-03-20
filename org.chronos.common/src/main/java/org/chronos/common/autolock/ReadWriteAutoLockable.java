package org.chronos.common.autolock;

/**
 * A {@link ReadWriteAutoLockable} is any object that can be locked {@linkplain #lockExclusive() exclusively} or
 * {@linkplain #lockNonExclusive() non-exclusively} using the <code>try-with-resources</code> pattern.
 *
 * <p>
 * See {@link #lockExclusive()} and {@link #lockNonExclusive()} for example usages.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ReadWriteAutoLockable {

	/**
	 * Declares that a thread is about to perform an exclusive task that can not run in parallel with other tasks.
	 *
	 * <p>
	 * All invocations of this method must adhere to the following usage pattern:
	 *
	 * <pre>
	 * try (LockHolder lock = lockable.lockExclusive()) {
	 * 	// ... perform tasks ...
	 * }
	 * </pre>
	 *
	 * This method ensures that exclusive operations are properly blocked when another operation is taking place.
	 *
	 * @return The object representing lock ownership. The lock ownership will be released once {@link AutoLock#close()}
	 *         is invoked, which is called automatically by the try-with-resources statement.
	 */
	public AutoLock lockExclusive();

	/**
	 * Declares that a thread is about to perform a non-exclusive task that can run in parallel with other non-exclusive
	 * locking tasks.
	 *
	 * <p>
	 * All invocations of this method must adhere to the following usage pattern:
	 *
	 * <pre>
	 * try (LockHolder lock = lockable.lockNonExclusive()) {
	 * 	// ... perform tasks ...
	 * }
	 * </pre>
	 *
	 * This method ensures that non-exclusive operations are properly blocked when an exclusive operation is taking
	 * place.
	 *
	 * @return The object representing lock ownership. The lock ownership will be released once {@link AutoLock#close()}
	 *         is invoked, which is called automatically by the try-with-resources statement.
	 */
	public AutoLock lockNonExclusive();

}
