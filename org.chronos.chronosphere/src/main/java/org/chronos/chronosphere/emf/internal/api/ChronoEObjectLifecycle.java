package org.chronos.chronosphere.emf.internal.api;

public enum ChronoEObjectLifecycle {

	TRANSIENT, PERSISTENT_CLEAN, PERSISTENT_MODIFIED, PERSISTENT_REMOVED;

	public boolean isTransient() {
		return TRANSIENT.equals(this);
	}

	public boolean isPersistent() {
		return this.isTransient() == false;
	}

	public boolean isRemoved() {
		return PERSISTENT_REMOVED.equals(this);
	}
}
