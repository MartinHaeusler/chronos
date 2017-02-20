package org.chronos.chronograph.internal.impl.structure.graph;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;

public class ChronoGraphIoRegistry extends AbstractIoRegistry {

	public static ChronoGraphIoRegistry INSTANCE = new ChronoGraphIoRegistry();

	private ChronoGraphIoRegistry() {
		// this.register(GraphSONIo.class, null, ChronoGraphSONModule.getInstance());
		//		this.register(GryoIo.class, ChronoVertexId.class, null);
		//		this.register(GryoIo.class, ChronoVertexPropertyId.class, null);
		//		this.register(GryoIo.class, ChronoEdgeId.class, null);
	}
}
