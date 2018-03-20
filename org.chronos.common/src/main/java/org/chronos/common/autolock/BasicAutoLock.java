package org.chronos.common.autolock;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.locks.Lock;

public class BasicAutoLock extends AbstractAutoLock {

	private final Lock lock;

	public BasicAutoLock(final Lock lock) {
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
