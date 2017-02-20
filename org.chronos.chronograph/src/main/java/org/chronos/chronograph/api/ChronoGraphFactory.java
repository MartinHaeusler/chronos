package org.chronos.chronograph.api;

import org.chronos.chronograph.api.builder.graph.ChronoGraphBaseBuilder;
import org.chronos.chronograph.internal.impl.factory.ChronoGraphFactoryImpl;

public interface ChronoGraphFactory {

	public static final ChronoGraphFactory INSTANCE = new ChronoGraphFactoryImpl();

	public ChronoGraphBaseBuilder create();

}
