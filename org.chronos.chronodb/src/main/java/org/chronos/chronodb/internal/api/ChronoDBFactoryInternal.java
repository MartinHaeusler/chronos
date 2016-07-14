package org.chronos.chronodb.internal.api;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBFactory;
import org.chronos.chronodb.api.exceptions.ChronoDBConfigurationException;
import org.chronos.chronodb.internal.impl.builder.database.ChronoDBFactoryImpl;

/**
 * Extended version of the {@link ChronoDBFactory} interface.
 *
 * <p>
 * This interface and its methods are for internal use only, are subject to change and are not considered to be part of
 * the public API. Down-casting objects to internal interfaces may cause application code to become incompatible with
 * future releases, and is therefore strongly discouraged.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDBFactoryInternal extends ChronoDBFactory {

	// =================================================================================================================
	// STATIC PART
	// =================================================================================================================

	/** The static factory instance. */
	public static final ChronoDBFactoryInternal INSTANCE = new ChronoDBFactoryImpl();

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	/**
	 * Creates a new {@link ChronoDB} instance based on the given configuration.
	 *
	 * @param configuration
	 *            The configuration to use. Must not be <code>null</code>.
	 * @return The newly created ChronoDB instance.
	 *
	 * @throws ChronoDBConfigurationException
	 *             Thrown if the configuration is invalid.
	 */
	public ChronoDB create(final ChronoDBConfiguration configuration) throws ChronoDBConfigurationException;

}
