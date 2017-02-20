package org.chronos.chronograph.api.builder.graph;

public interface ChronoGraphJdbcBuilder extends ChronoGraphFinalizableBuilder<ChronoGraphJdbcBuilder> {

	/**
	 * Specifies the login credentials to use when connecting to the JDBC database.
	 *
	 * <p>
	 * By default, no credentials are used at all.
	 *
	 * @param username
	 *            The username to use. Must not be <code>null</code>.
	 * @param password
	 *            The password to use. Must not be <code>null</code>.
	 * @return <code>this</code> (for fluent method chaining)
	 */
	public ChronoGraphJdbcBuilder withCredentials(String username, String password);

}
