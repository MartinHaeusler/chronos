package org.chronos.chronograph.internal.impl.builder.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronograph.api.builder.index.ElementTypeChoiceIndexBuilder;
import org.chronos.chronograph.api.builder.index.IndexBuilderStarter;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
import org.chronos.chronograph.internal.impl.index.IndexType;

public class ChronoGraphIndexBuilder implements IndexBuilderStarter {

	private final ChronoGraphIndexManagerInternal manager;

	public ChronoGraphIndexBuilder(final ChronoGraphIndexManagerInternal manager) {
		checkNotNull(manager, "Precondition violation - argument 'manager' must not be NULL!");
		this.manager = manager;
	}

	@Override
	public ElementTypeChoiceIndexBuilder stringIndex() {
		return new ElementTypeChoiceIndexBuilderImpl(this.manager, IndexType.STRING);
	}

	@Override
	public ElementTypeChoiceIndexBuilder longIndex() {
		return new ElementTypeChoiceIndexBuilderImpl(this.manager, IndexType.LONG);
	}

	@Override
	public ElementTypeChoiceIndexBuilder doubleIndex() {
		return new ElementTypeChoiceIndexBuilderImpl(this.manager, IndexType.DOUBLE);
	}

}
