package org.chronos.chronosphere.internal.configuration.api;

import org.chronos.common.configuration.ChronosConfiguration;

public interface ChronoSphereConfiguration extends ChronosConfiguration {

	// =====================================================================================================================
	// STATIC KEY NAMES
	// =====================================================================================================================

	public static final String NAMESPACE = "org.chronos.chronosphere";
	public static final String NS_DOT = NAMESPACE + '.';

	public static final String BATCH_INSERT__BATCH_SIZE = NS_DOT + "batchInsert.batchSize";

	// =================================================================================================================
	// GENERAL CONFIGURATION
	// =================================================================================================================

	public int getBatchInsertBatchSize();

}
