package org.chronos.chronograph.internal.impl.structure.graph;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.concurrent.Callable;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.transaction.ElementLoadMode;
import org.chronos.chronograph.internal.impl.transaction.GraphTransactionContext;

import com.google.common.collect.Iterators;

public abstract class AbstractChronoElement implements ChronoElement {

	protected final String id;
	protected String label;
	protected final ChronoGraph graph;

	protected final Thread owningThread;
	protected long loadedAtRollbackCount;
	protected ChronoGraphTransaction owningTransaction;

	protected boolean fullyLoaded;

	private int skipRemovedCheck;
	private int skipModificationCheck;

	private ElementLifecycleStatus status;

	protected AbstractChronoElement(final ChronoGraph g, final ChronoGraphTransaction tx, final String id,
			final String label, final boolean lazy) {
		checkNotNull(g, "Precondition violation - argument 'g' must not be NULL!");
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
		this.graph = g;
		this.owningTransaction = tx;
		this.loadedAtRollbackCount = tx.getRollbackCount();
		this.id = id;
		this.status = ElementLifecycleStatus.NEW;
		this.label = label;
		this.owningThread = Thread.currentThread();
		if (lazy) {
			this.fullyLoaded = false;
		} else {
			this.fullyLoaded = true;
		}
	}

	@Override
	public String id() {
		return this.id;
	}

	@Override
	public String label() {
		return this.label;
	}

	@Override
	public ChronoGraph graph() {
		return this.graph;
	}

	@Override
	public void remove() {
		this.checkAccess();
		// removing an element can start a transaction as well
		this.graph().tx().readWrite();
		this.updateLifecycleStatus(ElementLifecycleStatus.REMOVED);
	}

	@Override
	public boolean isRemoved() {
		return this.status.equals(ElementLifecycleStatus.REMOVED);
	}

	@Override
	public void updateLifecycleStatus(final ElementLifecycleStatus status) {
		if (this.isModificationCheckActive()) {
			if (ElementLifecycleStatus.REMOVED.equals(this.status) &&
					(status.equals(ElementLifecycleStatus.PROPERTY_CHANGED) || status.equals(ElementLifecycleStatus.EDGE_CHANGED))) {
				throw new IllegalStateException("Cannot switch '" + this.getClass().getSimpleName() + "' with id '" + this.id() + "' from REMOVED to " + status + " element lifecycle status!");
			}
			if (ElementLifecycleStatus.PROPERTY_CHANGED.equals(this.status) &&
					status.equals(ElementLifecycleStatus.EDGE_CHANGED)) {
				// property changes include edge changes
				return;
			}
			this.status = status;
		}
	}

	@Override
	public ElementLifecycleStatus getStatus() {
		return this.status;
	}

	// =================================================================================================================
	// HASH CODE & EQUALS
	// =================================================================================================================

	@Override
	public final int hashCode() {
		// according to TinkerGraph reference implementation
		return ElementHelper.hashCode(this);
	}

	@Override
	public final boolean equals(final Object object) {
		// according to TinkerGraph reference implementation
		return ElementHelper.areEqual(this, object);
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	@Override
	public ChronoGraphTransaction getOwningTransaction() {
		return this.owningTransaction;
	}

	public void setOwningTransaction(final ChronoGraphTransaction transaction) {
		checkNotNull(transaction, "Precondition violation - argument 'transaction' must not be NULL!");
		this.owningTransaction = transaction;
	}

	public Thread getOwningThread() {
		return this.owningThread;
	}

	protected ChronoVertex resolveVertex(final String id) {
		return (ChronoVertex) this.getOwningTransaction().getVertex(id, ElementLoadMode.LAZY);
	}

	protected ChronoEdge resolveEdge(final String id) {
		Iterator<Edge> iterator = this.graph.edges(id);
		return (ChronoEdge) Iterators.getOnlyElement(iterator);
	}

	protected ChronoGraphTransaction getGraphTransaction() {
		return this.graph().tx().getCurrentTransaction();
	}

	protected GraphTransactionContext getTransactionContext() {
		return this.getGraphTransaction().getContext();
	}

	protected boolean isFullyLoaded() {
		return this.fullyLoaded;
	}

	@Override
	public long getLoadedAtRollbackCount() {
		return this.loadedAtRollbackCount;
	}

	// =====================================================================================================================
	// INTERNAL UTILITY METHODS
	// =====================================================================================================================

	protected boolean isModificationCheckActive() {
		return this.skipModificationCheck <= 0;
	}

	@SuppressWarnings("deprecation")
	protected void assertNotRemoved() {
		if (this.isRemoved() && this.skipRemovedCheck <= 0) {
			throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
		}
	}

	protected void withoutRemovedCheck(final Runnable r) {
		this.skipRemovedCheck++;
		try {
			r.run();
		} finally {
			this.skipRemovedCheck--;
		}
	}

	protected <T> T withoutRemovedCheck(final Callable<T> c) {
		this.skipRemovedCheck++;
		try {
			try {
				return c.call();
			} catch (Exception e) {
				throw new RuntimeException("Error during method execution", e);
			}
		} finally {
			this.skipRemovedCheck--;
		}
	}

	protected void withoutModificationCheck(final Runnable r) {
		this.skipModificationCheck++;
		try {
			r.run();
		} finally {
			this.skipModificationCheck--;
		}
	}

	protected <T> T withoutModificationCheck(final Callable<T> c) {
		this.skipModificationCheck++;
		try {
			try {
				return c.call();
			} catch (Exception e) {
				throw new RuntimeException("Error during method execution", e);
			}
		} finally {
			this.skipModificationCheck--;
		}
	}

	public void checkAccess() {
		this.checkThread();
		this.checkTransaction();
		this.assertFullyLoaded();
		this.assertNotRemoved();
	}

	public void checkThread() {
		if (this.owningTransaction.isThreadedTx()) {
			// if we are owned by a threaded transaction, any thread has access to the element.
			return;
		}
		Thread currentThread = Thread.currentThread();
		if (currentThread.equals(this.getOwningThread()) == false) {
			throw new IllegalStateException("Graph Elements generated by a thread-bound transaction"
					+ " are not safe for concurrent access! Do not share them among threads! Owning thread is '"
					+ this.getOwningThread().getName() + "', current thread is '" + currentThread.getName() + "'.");
		}
	}

	public void checkTransaction() {
		if (this.owningTransaction.isThreadedTx()) {
			// threaded tx
			if (this.owningTransaction.isOpen() == false) {
				throw new IllegalStateException(
						"This graph element is bound to a Threaded Transaction, which was already closed. "
								+ "Cannot continue to operate on this element.");
			}
		} else {
			// thread-local tx
			this.graph.tx().readWrite();
			ChronoGraphTransaction currentTx = this.graph.tx().getCurrentTransaction();
			if (currentTx.equals(this.getOwningTransaction())) {
				// we are still on the same transaction that created this element
				// Check if a rollback has occurred
				if (this.loadedAtRollbackCount == currentTx.getRollbackCount()) {
					// same transaction, same rollback count -> nothing to do.
					return;
				} else {
					// a rollback has occurred; if the element was modified, we need to reload it
					if (this.status.isDirty()) {
						this.reloadFromDatabase();
						this.updateLifecycleStatus(ElementLifecycleStatus.PERSISTED);
						this.fullyLoaded = true;
					}
					this.loadedAtRollbackCount = currentTx.getRollbackCount();
				}
			} else {
				// we are on a different transaction, rebind to the new transaction and reload
				this.owningTransaction = currentTx;
				this.loadedAtRollbackCount = this.owningTransaction.getRollbackCount();
				this.reloadFromDatabase();
				this.updateLifecycleStatus(ElementLifecycleStatus.PERSISTED);
				this.fullyLoaded = true;
			}
		}

	}

	protected void assertFullyLoaded() {
		if (this.isFullyLoaded()) {
			return;
		}
		ChronoGraphTransaction currentTx = this.graph.tx().getCurrentTransaction();
		this.reloadFromDatabase();
		this.loadedAtRollbackCount = currentTx.getRollbackCount();
		this.fullyLoaded = true;
	}

	protected abstract void reloadFromDatabase();
}
