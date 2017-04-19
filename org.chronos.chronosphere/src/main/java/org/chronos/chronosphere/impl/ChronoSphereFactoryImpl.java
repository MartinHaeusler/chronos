package org.chronos.chronosphere.impl;

import org.chronos.chronosphere.api.ChronoSphereFactory;
import org.chronos.chronosphere.api.builder.repository.ChronoSphereBaseBuilder;
import org.chronos.chronosphere.internal.builder.repository.impl.ChronoSphereBaseBuilderImpl;

public class ChronoSphereFactoryImpl implements ChronoSphereFactory {

	@Override
	public ChronoSphereBaseBuilder create() {
		return new ChronoSphereBaseBuilderImpl();
	}

}
