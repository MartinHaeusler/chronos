package org.chronos.chronodb.internal.impl.builder.database;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.impl.ChronoDBConfigurationImpl;
import org.chronos.common.builder.AbstractChronoBuilder;
import org.chronos.common.builder.ChronoBuilder;
import org.chronos.common.configuration.ChronosConfigurationUtil;

public abstract class AbstractChronoDBBuilder<SELF extends ChronoBuilder<?>> extends AbstractChronoBuilder<SELF>
		implements ChronoBuilder<SELF> {

	protected ChronoDBConfiguration getConfiguration() {
		Configuration apacheConfig = new MapConfiguration(this.getProperties());
		return ChronosConfigurationUtil.build(apacheConfig, ChronoDBConfigurationImpl.class);
	}

}
