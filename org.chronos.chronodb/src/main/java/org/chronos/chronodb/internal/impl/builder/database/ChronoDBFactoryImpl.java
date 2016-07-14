package org.chronos.chronodb.internal.impl.builder.database;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.builder.database.ChronoDBBaseBuilder;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBFactoryInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.impl.engines.inmemory.InMemoryChronoDB;
import org.chronos.chronodb.internal.impl.engines.jdbc.JdbcChronoDB;
import org.chronos.chronodb.internal.impl.engines.mapdb.MapDBChronoDB;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

public class ChronoDBFactoryImpl implements ChronoDBFactoryInternal {

	// =================================================================================================================
	// INTERNAL UTILITY METHODS
	// =================================================================================================================

	@Override
	public ChronoDB create(final ChronoDBConfiguration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		ChronosBackend backendType = configuration.getBackendType();
		ChronoDBInternal db = null;
		switch (backendType) {
		case FILE:
			db = new MapDBChronoDB(configuration);
			break;
		case INMEMORY:
			db = new InMemoryChronoDB(configuration);
			break;
		case JDBC:
			db = new JdbcChronoDB(configuration);
			break;
		default:
			throw new UnknownEnumLiteralException(backendType);
		}
		db.postConstruct();
		return db;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public ChronoDBBaseBuilder create() {
		return new ChronoDBBaseBuilderImpl();
	}

}
