package org.chronos.chronodb.api.builder.database;

import org.chronos.chronodb.api.ChronoDB;

/**
 * A builder for instances of {@link ChronoDB} which store their data in a JDBC-compatible relational database.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDBJdbcBuilder extends ChronoDBFinalizableBuilder<ChronoDBJdbcBuilder> {

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
	public ChronoDBJdbcBuilder withCredentials(String username, String password);

}
