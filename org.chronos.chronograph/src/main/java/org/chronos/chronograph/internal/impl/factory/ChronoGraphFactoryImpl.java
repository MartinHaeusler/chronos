package org.chronos.chronograph.internal.impl.factory;

import static com.google.common.base.Preconditions.*;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactoryClass;
import org.chronos.chronograph.api.ChronoGraphFactory;
import org.chronos.chronograph.api.builder.graph.ChronoGraphBaseBuilder;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.impl.builder.graph.ChronoGraphBaseBuilderImpl;

public class ChronoGraphFactoryImpl implements ChronoGraphFactory {

	/**
	 * This method is <b>required</b> by Apache Tinkerpop. Creates a new graph instance.
	 *
	 * <p>
	 * This is referenced via {@link ChronoGraph}'s <code>@</code>{@link GraphFactoryClass} annotation and is required
	 * for the {@link GraphFactory} integration. The graph factory will look for this specific method via reflection.
	 *
	 * @param configuration
	 *            The configuration to use for the new graph. Must not be <code>null</code>.
	 *
	 * @return The new graph instance.
	 */
	public static ChronoGraph open(final Configuration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		return ChronoGraphFactory.INSTANCE.create().fromConfiguration(configuration).build();
	}

	@Override
	public ChronoGraphBaseBuilder create() {
		return new ChronoGraphBaseBuilderImpl();
	}

}
