package org.chronos.common.autolock;

/**
 * A {@link AutoLockable} is any object that can be locked {@linkplain #lock() locked} or using the
 * <code>try-with-resources</code> pattern.
 *
 * <p>
 * See {@link #lock()} for example usages.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface AutoLockable {

	/**
	 * Declares that a thread is about to perform an exclusive task that can not run in parallel with other tasks.
	 *
	 * <p>
	 * All invocations of this method must adhere to the following usage pattern:
	 *
	 * <pre>
	 * try (LockHolder lock = lockable.lock()) {
	 * 	// ... perform tasks ...
	 * }
	 * </pre>
	 *
	 * This method ensures that exclusive operations are properly blocked when another operation is taking place.
	 *
	 * @return The object representing lock ownership. The lock ownership will be released once {@link AutoLock#close()}
	 *         is invoked, which is called automatically by the try-with-resources statement.
	 */
	public AutoLock lock();

}
