package org.chronos.chronosphere.api;

import org.chronos.chronosphere.api.builder.repository.ChronoSphereBaseBuilder;
import org.chronos.chronosphere.impl.ChronoSphereFactoryImpl;

/**
 * This is the factory for {@link ChronoSphere} instances, and the entry point to the fluent builder API.
 *
 * <p>
 * You can get the singleton instance of this class via {@link ChronoSphere#FACTORY}, or via
 * {@link ChronoSphereFactory#INSTANCE}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoSphereFactory {

	/** The singleton instance of this factory. */
	public static ChronoSphereFactory INSTANCE = new ChronoSphereFactoryImpl();

	/**
	 * Creates a new builder for a {@link ChronoSphere} repository.
	 *
	 * @return The repository builder, for method chaining. Never <code>null</code>.
	 */
	public ChronoSphereBaseBuilder create();

}
