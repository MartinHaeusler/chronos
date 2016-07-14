package org.chronos.common.builder;

import java.io.File;

/**
 * A generic builder interface that allows setting properties as key-value pairs.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @param <SELF>
 *            The dynamic type of <code>this</code> to return for method chaining.
 */
public interface ChronoBuilder<SELF extends ChronoBuilder<?>> {

	/**
	 * Sets the given property on this builder to the given value.
	 *
	 * @param key
	 *            The key to set. Must not be <code>null</code>.
	 * @param value
	 *            The value to set. Must not be <code>null</code>.
	 * @return <code>this</code> (for fluent method chaining).
	 */
	public SELF withProperty(String key, String value);

	/**
	 * Loads the given properties file as configuration for this builder.
	 *
	 * <p>
	 * The file must fulfill all of the following conditions:
	 * <ul>
	 * <li>The file must be a file, not a directory.
	 * <li>The file must exist.
	 * <li>The file must be accessible.
	 * <li>The file name must end with <code>.properties</code>
	 * <li>The file contents must follow the default {@link java.util.Properties} format.
	 * </ul>
	 *
	 * @param file
	 *            The properties file to read. Must not be <code>null</code>, must adhere to all of the conditions
	 *            stated above.
	 * @return <code>this</code> (for fluent method chaining).
	 */
	public SELF withPropertiesFile(File file);

	/**
	 * Loads the properties file via the given path as configuration for this builder.
	 *
	 * <p>
	 * The file given by the path must fulfill all of the following conditions:
	 * <ul>
	 * <li>The file must be a file, not a directory.
	 * <li>The file must exist.
	 * <li>The file must be accessible.
	 * <li>The file name must end with <code>.properties</code>
	 * <li>The file contents must follow the default {@link java.util.Properties} format.
	 * </ul>
	 *
	 * @param filePath
	 *            The file path to the properties file to read. Must not be <code>null</code>, must adhere to all of the
	 *            conditions stated above.
	 * @return <code>this</code> (for fluent method chaining).
	 */
	public SELF withPropertiesFile(String filePath);

}
