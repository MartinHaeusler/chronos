package org.chronos.chronodb.internal.impl.query;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.builder.query.QueryBuilderFinalizer;
import org.chronos.chronodb.api.builder.query.QueryBuilderStarter;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.internal.api.query.QueryManager;
import org.chronos.chronodb.internal.api.query.QueryOptimizer;
import org.chronos.chronodb.internal.api.query.QueryParser;
import org.chronos.chronodb.internal.impl.builder.query.StandardQueryBuilder;
import org.chronos.chronodb.internal.impl.builder.query.StandardQueryBuilderFinalizer;
import org.chronos.chronodb.internal.impl.query.optimizer.StandardQueryOptimizer;
import org.chronos.chronodb.internal.impl.query.parser.StandardQueryParser;

public class StandardQueryManager implements QueryManager {

	private final ChronoDBInternal owningDB;
	private final StandardQueryParser queryParser;
	private final StandardQueryOptimizer queryOptimizer;

	public StandardQueryManager(final ChronoDBInternal owningDB) {
		checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
		this.owningDB = owningDB;
		this.queryParser = new StandardQueryParser();
		this.queryOptimizer = new StandardQueryOptimizer();
	}

	@Override
	public QueryParser getQueryParser() {
		return this.queryParser;
	}

	@Override
	public QueryOptimizer getQueryOptimizer() {
		return this.queryOptimizer;
	}

	@Override
	public QueryBuilderStarter createQueryBuilder(final ChronoDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return new StandardQueryBuilder(this.owningDB, tx);
	}

	@Override
	public QueryBuilderFinalizer createQueryBuilderFinalizer(final ChronoDBTransaction tx, final ChronoDBQuery query) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
		return new StandardQueryBuilderFinalizer(this.owningDB, tx, query);
	}

}
