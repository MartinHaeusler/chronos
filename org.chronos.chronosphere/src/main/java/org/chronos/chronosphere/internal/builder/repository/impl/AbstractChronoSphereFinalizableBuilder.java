package org.chronos.chronosphere.internal.builder.repository.impl;

import org.apache.commons.configuration.Configuration;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.builder.repository.ChronoSphereFinalizableBuilder;
import org.chronos.chronosphere.impl.StandardChronoSphere;

public abstract class AbstractChronoSphereFinalizableBuilder<SELF extends ChronoSphereFinalizableBuilder<?>>
		extends AbstractChronoSphereBuilder<SELF> implements ChronoSphereFinalizableBuilder<SELF> {

	@Override
	public ChronoSphere build() {
		// always disable auto-TX in the graph for ChronoSphere
		this.withProperty(ChronoGraphConfiguration.TRANSACTION_AUTO_OPEN, "false");
		Configuration config = this.getPropertiesAsConfiguration();
		ChronoGraph graph = ChronoGraph.FACTORY.create().fromConfiguration(config).build();
		return new StandardChronoSphere(graph, config);
	}
}
