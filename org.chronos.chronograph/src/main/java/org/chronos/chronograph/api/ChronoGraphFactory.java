package org.chronos.chronograph.api;

import org.chronos.chronograph.api.builder.graph.ChronoGraphBaseBuilder;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.impl.factory.ChronoGraphFactoryImpl;

/**
 * The graph factory is responsible for creating new {@link ChronoGraph} instances.
 *
 * <p>
 * You can use the {@link #INSTANCE} constant directly, or alternatively use {@link ChronoGraph#FACTORY} to get access to the singleton instance of this class.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoGraphFactory {

	/** The singleton instance of the graph factory interface. */
	public static final ChronoGraphFactory INSTANCE = new ChronoGraphFactoryImpl();

	/**
	 * Entry point to the fluent API for creating new {@link ChronoGraph} instances.
	 *
	 * @return The fluent builder for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphBaseBuilder create();

}
