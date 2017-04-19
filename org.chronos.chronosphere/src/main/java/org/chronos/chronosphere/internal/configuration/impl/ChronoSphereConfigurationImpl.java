package org.chronos.chronosphere.internal.configuration.impl;

import org.chronos.chronosphere.internal.configuration.api.ChronoSphereConfiguration;
import org.chronos.common.configuration.AbstractConfiguration;
import org.chronos.common.configuration.annotation.Namespace;
import org.chronos.common.configuration.annotation.Parameter;

@Namespace(ChronoSphereConfiguration.NAMESPACE)
public class ChronoSphereConfigurationImpl extends AbstractConfiguration implements ChronoSphereConfiguration {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	@Parameter(key = BATCH_INSERT__BATCH_SIZE, optional = true)
	private int batchInsertBatchSize = 10_000;

	// =====================================================================================================================
	// GETTERS & SETTERS
	// =====================================================================================================================

	@Override
	public int getBatchInsertBatchSize() {
		return this.batchInsertBatchSize;
	}

}
