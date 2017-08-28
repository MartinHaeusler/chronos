package org.chronos.chronograph.internal.impl.builder.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronograph.api.builder.index.GraphElementIndexBuilder;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
import org.chronos.chronograph.internal.impl.index.IndexType;

public abstract class AbstractGraphElementIndexBuilder<SELF extends GraphElementIndexBuilder<SELF>>
		implements GraphElementIndexBuilder<SELF> {

	protected final ChronoGraphIndexManagerInternal manager;
	protected final String propertyName;
	protected final IndexType indexType;

	protected AbstractGraphElementIndexBuilder(final ChronoGraphIndexManagerInternal manager,
			final String propertyName, final IndexType indexType) {
		checkNotNull(manager, "Precondition violation - argument 'manager' must not be NULL!");
		checkNotNull(propertyName, "Precondition violation - argument 'propertyName' must not be NULL!");
		checkNotNull(indexType, "Precondition violation - argument 'indexType' must not be NULL!");
		this.manager = manager;
		this.propertyName = propertyName;
		this.indexType = indexType;
	}

}
