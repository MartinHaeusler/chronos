package org.chronos.chronodb.internal.impl.lock;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.locks.Lock;

public class BasicLockHolder extends AbstractLockHolder {

	private final Lock lock;

	public BasicLockHolder(final Lock lock) {
		checkNotNull(lock, "Precondition violation - argument 'lock' must not be NULL!");
		this.lock = lock;
	}

	@Override
	protected void doLock() {
		this.lock.lock();
	}

	@Override
	protected void doUnlock() {
		this.lock.unlock();
	}

}
