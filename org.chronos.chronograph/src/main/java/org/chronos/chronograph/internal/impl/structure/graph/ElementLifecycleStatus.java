package org.chronos.chronograph.internal.impl.structure.graph;

public enum ElementLifecycleStatus {

	/** The element was newly added in this transaction. It has never been persisted before. */
	NEW,
	/** The element is in-sync with the persistence backend and unchanged. */
	PERSISTED,
	/** The element was removed by the user in this transaction. */
	REMOVED,
	/** Properties of the element have changed. This includes also edge changes. */
	PROPERTY_CHANGED,
	/** Edges of the vertex have changed. Properties are unchanged so far. */
	EDGE_CHANGED;

	public boolean isDirty() {
		return this.equals(PERSISTED) == false;
	}
}
