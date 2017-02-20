package org.chronos.chronograph.internal.impl.builder.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronograph.api.builder.index.GraphElementIndexBuilder;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;

public abstract class AbstractGraphElementIndexBuilder<SELF extends GraphElementIndexBuilder<SELF>>
		implements GraphElementIndexBuilder<SELF> {

	protected final ChronoGraphIndexManagerInternal manager;
	protected final String propertyName;

	protected AbstractGraphElementIndexBuilder(final ChronoGraphIndexManagerInternal manager,
			final String propertyName) {
		checkNotNull(manager, "Precondition violation - argument 'manager' must not be NULL!");
		checkNotNull(propertyName, "Precondition violation - argument 'propertyName' must not be NULL!");
		this.manager = manager;
		this.propertyName = propertyName;
	}

}
