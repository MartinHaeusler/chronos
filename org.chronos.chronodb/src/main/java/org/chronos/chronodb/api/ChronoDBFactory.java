package org.chronos.chronodb.api;

import org.chronos.chronodb.api.builder.database.ChronoDBBaseBuilder;
import org.chronos.chronodb.internal.impl.builder.database.ChronoDBFactoryImpl;

/**
 * Acts as a singleton factory for instances of {@link ChronoDB}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoDBFactory {

	// =================================================================================================================
	// STATIC PART
	// =================================================================================================================

	/** The static factory instance. */
	public static final ChronoDBFactory INSTANCE = new ChronoDBFactoryImpl();

	// =================================================================================================================
	// API METHODS
	// =================================================================================================================

	/**
	 * Starting point of the fluent API for database construction.
	 *
	 * @return The fluent database builder. Never <code>null</code>.
	 */
	public ChronoDBBaseBuilder create();

}
