package org.chronos.chronograph.internal.api.configuration;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.common.configuration.ChronosConfiguration;

/**
 * This class represents the configuration of a single {@link ChronoGraph} instance.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoGraphConfiguration extends ChronosConfiguration {

	// =====================================================================================================================
	// STATIC KEY NAMES
	// =====================================================================================================================

	public static final String NAMESPACE = "org.chronos.chronograph";
	public static final String NS_DOT = NAMESPACE + '.';

	public static final String TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD = NS_DOT + "transaction.checkIdExistenceOnAdd";
	public static final String TRANSACTION_AUTO_OPEN = NS_DOT + "transaction.autoOpen";

	// =================================================================================================================
	// GENERAL CONFIGURATION
	// =================================================================================================================

	/**
	 * Checks if graph {@link Element} IDs provided by the user should be checked in the backend for duplicates or not.
	 *
	 * <p>
	 * This property has the following implications:
	 * <ul>
	 * <li><b>When it is <code>true</code>:</b><br>
	 * This is the "trusted" mode. The user of the API will be responsible for providing unique identifiers for the
	 * graph elements. No additional checking will be performed before the ID is being used to instantiate a graph
	 * element. This mode allows for faster graph element creation, but inconsistencies may be introduced if the user
	 * provides duplicate IDs.
	 * <li><b>When it is <code>false</code>:</b><br>
	 * This is the "untrusted" mode. Before using a user-provided ID for a new graph element, a check will be performed
	 * in the underlying persistence if an element with that ID already exists. If such an element already exists, an
	 * exception will be thrown and the ID will not be used. This mode is slower when creating new graph elements due to
	 * the additional check, but it is also safer in that it warns the user early that a duplicate ID exists.
	 * </ul>
	 *
	 * Regardless whether this setting is on or off, when the user provides no custom ID for a graph element, a new
	 * UUID-based identifier will be generated automatically.
	 *
	 * @return <code>true</code> if a check for duplicated IDs should be performed, or <code>false</code> if that check
	 *         should be skipped.
	 */
	public boolean isCheckIdExistenceOnAddEnabled();

	/**
	 * Checks if auto-opening of graph transactions is enabled or not.
	 * 
	 * @return <code>true</code> if auto-opening of graph transactions is enabled, otherwise <code>false</code>.
	 */
	public boolean isTransactionAutoOpenEnabled();

}
