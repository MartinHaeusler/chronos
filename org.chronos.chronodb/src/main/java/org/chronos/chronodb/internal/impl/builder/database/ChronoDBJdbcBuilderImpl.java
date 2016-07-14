package org.chronos.chronodb.internal.impl.builder.database;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.builder.database.ChronoDBJdbcBuilder;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;

public class ChronoDBJdbcBuilderImpl extends AbstractChronoDBFinalizableBuilder<ChronoDBJdbcBuilder>
		implements ChronoDBJdbcBuilder {

	public ChronoDBJdbcBuilderImpl(final String jdbcConnectionURL) {
		checkNotNull(jdbcConnectionURL, "Precondition violation - argument 'jdbcConnectionURL' must not be NULL!");
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.JDBC.toString());
		this.withProperty(ChronoDBConfiguration.JDBC_CONNECTION_URL, jdbcConnectionURL);
	}

	@Override
	public ChronoDBJdbcBuilder withCredentials(final String username, final String password) {
		checkNotNull(username, "Precondition violation - argument 'username' must not be NULL!");
		checkNotNull(password, "Precondition violation - argument 'password' must not be NULL!");
		this.withProperty(ChronoDBConfiguration.JDBC_CREDENTIALS_USERNAME, username);
		this.withProperty(ChronoDBConfiguration.JDBC_CREDENTIALS_PASSWORD, password);
		return this;
	}

}
