package org.chronos.common.buildinfo;

public interface ChronosBuildConfiguration {

	// =====================================================================================================================
	// STATIC KEY NAMES
	// =====================================================================================================================

	public static final String NAMESPACE = "org.chronos";
	public static final String NS_DOT = NAMESPACE + '.';

	public static final String BUILD_VERSION = NS_DOT + "buildVersion";

	// =================================================================================================================
	// GENERAL CONFIGURATION
	// =================================================================================================================

	public String getBuildVersion();

}
