package org.chronos.chronodb.internal.impl.builder.query;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;

public class StandardQueryBuilderFinalizer extends AbstractFinalizableQueryBuilder {

	private final ChronoDBInternal owningDB;
	private final ChronoDBTransaction tx;

	private final ChronoDBQuery query;

	public StandardQueryBuilderFinalizer(final ChronoDBInternal owningDB, final ChronoDBTransaction tx,
			final ChronoDBQuery query) {
		checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
		this.owningDB = owningDB;
		this.tx = tx;
		this.query = query;
	}

	@Override
	protected ChronoDBInternal getOwningDB() {
		return this.owningDB;
	}

	@Override
	protected ChronoDBTransaction getTx() {
		return this.tx;
	}

	@Override
	protected ChronoDBQuery getQuery() {
		return this.query;
	}

}
