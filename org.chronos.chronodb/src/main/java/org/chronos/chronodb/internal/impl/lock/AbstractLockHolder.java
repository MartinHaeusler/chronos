package org.chronos.chronodb.internal.impl.lock;

import org.chronos.chronodb.internal.api.Lockable.LockHolder;
import org.chronos.common.logging.ChronoLogger;

public abstract class AbstractLockHolder implements LockHolder {

	private int timesLockAcquired;

	protected AbstractLockHolder() {
		this.timesLockAcquired = 0;
	}

	@Override
	public final void acquireLock() {
		if (this.timesLockAcquired == 0) {
			this.doLock();
			this.timesLockAcquired = 1;
		} else {
			this.timesLockAcquired++;
		}
	}

	@Override
	public final void releaseLock() {
		if (this.timesLockAcquired > 1) {
			this.timesLockAcquired--;
		} else if (this.timesLockAcquired == 1) {
			this.doUnlock();
			this.timesLockAcquired = 0;
		} else {
			throw new IllegalStateException("Attempted to 'unlock' a LockHolder that was not 'lock'ed before!");
		}
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (this.timesLockAcquired > 0) {
			ChronoLogger.logWarning("WARNING! Non-released lock is being GC'ed! Releasing it now.");
			this.close();
		}
	}

	protected abstract void doLock();

	protected abstract void doUnlock();

}