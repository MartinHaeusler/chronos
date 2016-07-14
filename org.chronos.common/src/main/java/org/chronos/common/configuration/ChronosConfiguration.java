package org.chronos.common.configuration;

import java.util.Map;

import org.apache.commons.configuration.Configuration;

/**
 * This is the basic interface for all configuration classes in Chronos.
 *
 * <p>
 * The primary purpose of this interface (aside from acting as a base class) is to offer methods that convert the
 * configuration into a variety of output formats, such as an Apache Commons {@link Configuration} object.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronosConfiguration {

	/**
	 * Converts the contents of this configuration into an Apache Commons {@link Configuration} object.
	 *
	 * <p>
	 * The returned object will receive a copy of the internal values, therefore any modifications to the returned
	 * configuration will have no impact on the internal state, and vice versa.
	 *
	 * @return The Apache Commons Configuration object. May be empty, but never <code>null</code>.
	 */
	public Configuration asCommonsConfiguration();

	/**
	 * Converts the contents of this configuration into a regular <code>java.util.{@link Map}</code>.
	 *
	 * <p>
	 * The returned map will receive a copy of the internal values, therefore any modifications to the returned map will
	 * have no impact on the internal state, and vice versa.
	 *
	 * @return This configuration, converted into a regular map. May be empty, but never <code>null</code>.
	 */
	public Map<String, Object> asMap();

}
