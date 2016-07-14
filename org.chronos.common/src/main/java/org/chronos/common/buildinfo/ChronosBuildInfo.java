package org.chronos.common.buildinfo;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.chronos.common.configuration.ChronosConfigurationUtil;
import org.chronos.common.util.ClasspathUtils;

public class ChronosBuildInfo {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	private static final String BUILD_INFO_FILE_NAME = "buildInfo.properties";

	private static ChronosBuildConfiguration CONFIGURATION;

	public static synchronized ChronosBuildConfiguration getConfiguration() {
		if (CONFIGURATION == null) {
			try {
				File buildInfoFile = ClasspathUtils.getResourceAsFile(BUILD_INFO_FILE_NAME);
				if (buildInfoFile == null || buildInfoFile.exists() == false || buildInfoFile.isFile() == false) {
					throw new IllegalStateException("Unable to retrieve file '" + BUILD_INFO_FILE_NAME + "'!");
				}
				Configuration apacheConfig = new PropertiesConfiguration(buildInfoFile);
				CONFIGURATION = ChronosConfigurationUtil.build(apacheConfig, ChronosBuildConfigurationImpl.class);
			} catch (Exception e) {
				throw new IllegalStateException(
						"An exception occurred when reading the Chronos Build Configuration. See root cause for details.",
						e);
			}
		}
		return CONFIGURATION;
	}
}
