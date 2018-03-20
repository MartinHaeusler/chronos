package org.chronos.chronosphere.api.builder.repository;

import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.common.builder.ChronoBuilder;

public interface ChronoSphereFinalizableBuilder<SELF extends ChronoSphereFinalizableBuilder<?>>
		extends ChronoBuilder<SELF> {

	/**
	 * Builds the {@link ChronoSphere} instance, using the properties specified by the fluent API.
	 *
	 * <p>
	 * This method finalizes the buildLRU process. Afterwards, the builder should be discarded.
	 *
	 * @return The new {@link ChronoSphere} instance. Never <code>null</code>.
	 */
	public ChronoSphere build();
}
