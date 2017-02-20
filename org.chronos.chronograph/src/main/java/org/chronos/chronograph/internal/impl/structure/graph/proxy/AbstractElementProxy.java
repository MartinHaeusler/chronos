package org.chronos.chronograph.internal.impl.structure.graph.proxy;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.structure.graph.AbstractChronoElement;
import org.chronos.chronograph.internal.impl.structure.graph.ElementLifecycleStatus;

public abstract class AbstractElementProxy<T extends ChronoElement> implements ChronoElement {

	protected final String id;
	protected final ChronoGraph graph;

	protected T element;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected AbstractElementProxy(final ChronoGraph graph, final String id) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		this.graph = graph;
		this.id = id;
	}

	// =================================================================================================================
	// TINKERPOP API
	// =================================================================================================================

	@Override
	public String label() {
		return this.getElement().label();
	}

	@Override
	public void remove() {
		this.getElement().remove();
	}

	@Override
	public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
		return this.getElement().properties(propertyKeys);
	}

	@Override
	public String id() {
		return this.id;
	}

	@Override
	public ChronoGraph graph() {
		return this.graph;
	}

	@Override
	public void removeProperty(final String key) {
		this.getElement().removeProperty(key);
	}

	@Override
	public boolean isRemoved() {
		return this.getElement().isRemoved();
	}

	@Override
	public void updateLifecycleStatus(final ElementLifecycleStatus status) {
		this.getElement().updateLifecycleStatus(status);
	}

	@Override
	public ElementLifecycleStatus getStatus() {
		return this.getElement().getStatus();
	}

	@Override
	public ChronoGraphTransaction getOwningTransaction() {
		return this.getElement().getOwningTransaction();
	}

	@Override
	public long getLoadedAtRollbackCount() {
		return this.getElement().getLoadedAtRollbackCount();
	}

	@Override
	public int hashCode() {
		return ElementHelper.hashCode(this);
	}

	@Override
	public boolean equals(final Object obj) {
		return ElementHelper.areEqual(this, obj);
	}

	// =================================================================================================================
	// INTERNAL UTILITY METHODS
	// =================================================================================================================

	public T getElement() {
		this.assertElementLoaded();
		return this.element;
	}

	protected void assertElementLoaded() {
		this.graph.tx().readWrite();
		if (this.element != null) {
			// make sure that no other thread tries to access this element
			((AbstractChronoElement) this.element).checkThread();
			// check that the element belongs to the current transaction
			ChronoGraphTransaction currentTx = this.graph.tx().getCurrentTransaction();
			ChronoGraphTransaction owningTx = this.element.getOwningTransaction();
			if (currentTx.equals(owningTx)) {
				// check if the rollback count is still the same
				if (this.element.getLoadedAtRollbackCount() == currentTx.getRollbackCount()) {
					// still same transaction, same rollback count, keep the element as-is.
					return;
				} else {
					// a rollback has occurred, set the element to NULL and re-load
					this.element = null;
				}
			} else {
				// the transaction that created our element has been closed; set the element to NULL and re-load
				this.element = null;
			}
		}
		this.graph.tx().readWrite();
		ChronoGraphTransaction tx = this.graph.tx().getCurrentTransaction();
		this.element = this.loadElement(tx, this.id);
		this.registerProxyAtTransaction(tx);
	}

	// =================================================================================================================
	// ABSTRACT METHOD DECLARATIONS
	// =================================================================================================================

	protected abstract T loadElement(final ChronoGraphTransaction tx, final String id);

	protected abstract void registerProxyAtTransaction(ChronoGraphTransaction transaction);

}
