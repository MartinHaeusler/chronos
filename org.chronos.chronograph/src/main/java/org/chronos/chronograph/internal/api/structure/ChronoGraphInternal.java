package org.chronos.chronograph.internal.api.structure;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.common.autolock.AutoLock;

public interface ChronoGraphInternal extends ChronoGraph {

	/**
	 * Returns the {@link ChronoDB} instance that acts as the backing store for this {@link ChronoGraph}.
	 *
	 * @return The backing ChronoDB instance. Never <code>null</code>.
	 */
	public ChronoDB getBackingDB();

	/**
	 * Acquires the commit lock.
	 *
	 * <p>
	 * Use this in conjunction with <code>try-with-resources</code> statements for easy locking.
	 *
	 * @return The auto-closable commit lock. Never <code>null</code>.
	 */
	public AutoLock commitLock();

}
