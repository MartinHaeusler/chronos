package org.chronos.chronodb.internal.impl.query.parser.ast;

import org.chronos.chronodb.internal.api.query.ChronoDBQuery;

public class ChronoDBQueryImpl implements ChronoDBQuery {

	private final String keyspace;
	private final QueryElement rootElement;

	public ChronoDBQueryImpl(final String keyspace, final QueryElement element) {
		this.keyspace = keyspace;
		this.rootElement = element;
	}

	@Override
	public String getKeyspace() {
		return this.keyspace;
	}

	@Override
	public QueryElement getRootElement() {
		return this.rootElement;
	}

	@Override
	public String toString() {
		return "Query[keyspace='" + this.keyspace + "', AST=" + this.rootElement + "]";
	}

}
