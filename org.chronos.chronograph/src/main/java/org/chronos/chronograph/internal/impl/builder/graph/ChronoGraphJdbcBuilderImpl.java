package org.chronos.chronograph.internal.impl.builder.graph;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronograph.api.builder.graph.ChronoGraphJdbcBuilder;

public class ChronoGraphJdbcBuilderImpl extends AbstractChronoGraphFinalizableBuilder<ChronoGraphJdbcBuilder> implements ChronoGraphJdbcBuilder {

	public ChronoGraphJdbcBuilderImpl(final String jdbcURL) {
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.JDBC.toString());
		this.withProperty(ChronoDBConfiguration.JDBC_CONNECTION_URL, jdbcURL);
	}

	@Override
	public ChronoGraphJdbcBuilder withCredentials(final String username, final String password) {
		checkNotNull(username, "Precondition violation - argument 'username' must not be NULL!");
		checkNotNull(password, "Precondition violation - argument 'password' must not be NULL!");
		this.withProperty(ChronoDBConfiguration.JDBC_CREDENTIALS_USERNAME, username);
		this.withProperty(ChronoDBConfiguration.JDBC_CREDENTIALS_PASSWORD, password);
		return this;
	}

}
