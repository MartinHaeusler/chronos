package org.chronos.common.buildinfo;

import org.chronos.common.configuration.AbstractConfiguration;
import org.chronos.common.configuration.annotation.Namespace;
import org.chronos.common.configuration.annotation.Parameter;

@Namespace(ChronosBuildConfiguration.NAMESPACE)
public class ChronosBuildConfigurationImpl extends AbstractConfiguration implements ChronosBuildConfiguration {

	// =====================================================================================================================
	// PROPERTIES
	// =====================================================================================================================

	@Parameter(key = ChronosBuildConfiguration.BUILD_VERSION, optional = false)
	private String chronosVersion;

	// =====================================================================================================================
	// GETTERS
	// =====================================================================================================================

	@Override
	public String getBuildVersion() {
		return this.chronosVersion;
	}
}
