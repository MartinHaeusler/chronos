package org.chronos.chronograph.api.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.structure.graph.ElementLifecycleStatus;

/**
 * A {@link ChronoElement} is a {@link ChronoGraph}-specific extension of the general TinkerPop {@link Element} class.
 *
 * <p>
 * This interface defines additional methods for all graph elements, mostly intended for internal use only.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoElement extends Element {

	/**
	 * Gets the unique identifier for the graph {@code ChronoElement}.
	 *
	 * <p>
	 * This method has been redefined from {@link Element#id()} to have a return type of {@link String} (instead of {@link Object}).
	 *
	 * @return The id of the element. Never <code>null</code>.
	 */
	@Override
	public String id();

	/**
	 * Get the {@link ChronoGraph} that this element is within.
	 *
	 * @return The graph of this element. Never <code>null</code>.
	 */
	@Override
	public ChronoGraph graph();

	/**
	 * Remove the property with the given key (name) from this element.
	 *
	 * @param key
	 *            The key (name) of the property to remove. Must not be <code>null</code>.
	 */
	public void removeProperty(String key);

	/**
	 * Checks if this element has been {@linkplain #remove() removed} from the graph or not.
	 *
	 * @return <code>true</code> if this element has been removed from the graph, or <code>true</code> if it is still a member of the graph.
	 */
	public boolean isRemoved();

	/**
	 * Sets the lifecycle status on this element.
	 *
	 * <i>For internal use only</i>.
	 *
	 * @param status
	 *            The new lifecycle status to set. Must not be <code>null</code>.
	 */
	public void updateLifecycleStatus(final ElementLifecycleStatus status);

	/**
	 * Returns the current lifecycle status of this element.
	 *
	 * @return the lifecycle status. Never <code>null</code>.
	 */
	public ElementLifecycleStatus getStatus();

	/**
	 * Returns the {@link ChronoGraphTransaction} to which this element belongs.
	 *
	 * @return The owning transaction. Never <code>null</code>.
	 */
	public ChronoGraphTransaction getOwningTransaction();

	/**
	 * Returns the state of the transaction rollback counter that was valid at the time when this element was loaded.
	 *
	 * @return The number of rollbacks done on the graph transaction before loading this element. Always greater than or equal to zero.
	 */
	public long getLoadedAtRollbackCount();
}
