package org.chronos.chronograph.api.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.structure.graph.ElementLifecycleStatus;

public interface ChronoElement extends Element {

	@Override
	public String id();

	@Override
	public ChronoGraph graph();

	public void removeProperty(String key);

	public boolean isRemoved();

	public void updateLifecycleStatus(final ElementLifecycleStatus status);

	public ElementLifecycleStatus getStatus();

	public ChronoGraphTransaction getOwningTransaction();

	public long getLoadedAtRollbackCount();
}
