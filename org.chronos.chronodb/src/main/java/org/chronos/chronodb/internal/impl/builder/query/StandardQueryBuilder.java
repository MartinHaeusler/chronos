package org.chronos.chronodb.internal.impl.builder.query;

import static com.google.common.base.Preconditions.*;

import java.util.List;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.builder.query.FinalizableQueryBuilder;
import org.chronos.chronodb.api.builder.query.QueryBuilder;
import org.chronos.chronodb.api.builder.query.QueryBuilderStarter;
import org.chronos.chronodb.api.builder.query.WhereBuilder;
import org.chronos.chronodb.api.exceptions.ChronoDBQuerySyntaxException;
import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.internal.api.query.QueryManager;
import org.chronos.chronodb.internal.api.query.QueryOptimizer;
import org.chronos.chronodb.internal.api.query.QueryParser;
import org.chronos.chronodb.internal.api.query.QueryTokenStream;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.parser.token.AndToken;
import org.chronos.chronodb.internal.impl.query.parser.token.BeginToken;
import org.chronos.chronodb.internal.impl.query.parser.token.EndOfInputToken;
import org.chronos.chronodb.internal.impl.query.parser.token.EndToken;
import org.chronos.chronodb.internal.impl.query.parser.token.KeyspaceToken;
import org.chronos.chronodb.internal.impl.query.parser.token.NotToken;
import org.chronos.chronodb.internal.impl.query.parser.token.OrToken;
import org.chronos.chronodb.internal.impl.query.parser.token.QueryToken;
import org.chronos.chronodb.internal.impl.query.parser.token.WhereToken;

import com.google.common.collect.Lists;

public class StandardQueryBuilder implements QueryBuilderStarter {

	private final ChronoDBInternal owningDB;
	private final ChronoDBTransaction tx;

	private final List<QueryToken> tokenList;

	private WhereToken currentWhereToken;

	private final QueryBuilderImpl queryBuilder;
	private final WhereBuilderImpl whereBuilder;
	private final FinalizableQueryBuilderImpl finalizableBuilder;

	public StandardQueryBuilder(final ChronoDBInternal owningDB, final ChronoDBTransaction tx) {
		checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		this.owningDB = owningDB;
		this.tx = tx;
		this.tokenList = Lists.newArrayList();
		this.queryBuilder = new QueryBuilderImpl();
		this.whereBuilder = new WhereBuilderImpl();
		this.finalizableBuilder = new FinalizableQueryBuilderImpl();
	}

	@Override
	public QueryBuilder inKeyspace(final String keyspace) {
		QueryToken keyspaceToken = new KeyspaceToken(keyspace);
		this.tokenList.add(keyspaceToken);
		return this.queryBuilder;
	}

	@Override
	public QueryBuilder inDefaultKeyspace() {
		QueryToken keyspaceToken = new KeyspaceToken(ChronoDBConstants.DEFAULT_KEYSPACE_NAME);
		this.tokenList.add(keyspaceToken);
		return this.queryBuilder;
	}

	private class QueryBuilderImpl implements QueryBuilder {

		@Override
		public QueryBuilder begin() {
			QueryToken beginToken = new BeginToken();
			StandardQueryBuilder.this.tokenList.add(beginToken);
			return this;
		}

		@Override
		public QueryBuilder end() {
			QueryToken endToken = new EndToken();
			StandardQueryBuilder.this.tokenList.add(endToken);
			return this;
		}

		@Override
		public QueryBuilder not() {
			QueryToken notToken = new NotToken();
			StandardQueryBuilder.this.tokenList.add(notToken);
			return this;
		}

		@Override
		public WhereBuilder where(final String indexName) {
			if (StandardQueryBuilder.this.currentWhereToken != null) {
				// this should never happen as such a query won't even compile in Java
				throw new ChronoDBQuerySyntaxException("Unfinished WHERE clause detected!");
			}
			WhereToken whereToken = new WhereToken();
			whereToken.setIndexName(indexName);
			StandardQueryBuilder.this.currentWhereToken = whereToken;
			StandardQueryBuilder.this.tokenList.add(whereToken);
			return StandardQueryBuilder.this.whereBuilder;
		}

	}

	private class WhereBuilderImpl implements WhereBuilder {

		@Override
		public FinalizableQueryBuilder contains(final String text) {
			checkNotNull(text, "Precondition violation - argument 'text' must not be NULL!");
			return this.addWhereDetails(Condition.CONTAINS, TextMatchMode.STRICT, text);
		}

		@Override
		public FinalizableQueryBuilder containsIgnoreCase(final String text) {
			checkNotNull(text, "Precondition violation - argument 'text' must not be NULL!");
			return this.addWhereDetails(Condition.CONTAINS, TextMatchMode.CASE_INSENSITIVE, text);
		}

		@Override
		public FinalizableQueryBuilder notContains(final String text) {
			checkNotNull(text, "Precondition violation - argument 'text' must not be NULL!");
			return this.addWhereDetails(Condition.NOT_CONTAINS, TextMatchMode.STRICT, text);
		}

		@Override
		public FinalizableQueryBuilder notContainsIgnoreCase(final String text) {
			checkNotNull(text, "Precondition violation - argument 'text' must not be NULL!");
			return this.addWhereDetails(Condition.NOT_CONTAINS, TextMatchMode.CASE_INSENSITIVE, text);
		}

		@Override
		public FinalizableQueryBuilder startsWith(final String text) {
			checkNotNull(text, "Precondition violation - argument 'text' must not be NULL!");
			return this.addWhereDetails(Condition.STARTS_WITH, TextMatchMode.STRICT, text);
		}

		@Override
		public FinalizableQueryBuilder startsWithIgnoreCase(final String text) {
			checkNotNull(text, "Precondition violation - argument 'text' must not be NULL!");
			return this.addWhereDetails(Condition.STARTS_WITH, TextMatchMode.CASE_INSENSITIVE, text);
		}

		@Override
		public FinalizableQueryBuilder notStartsWith(final String text) {
			checkNotNull(text, "Precondition violation - argument 'text' must not be NULL!");
			return this.addWhereDetails(Condition.NOT_STARTS_WITH, TextMatchMode.STRICT, text);
		}

		@Override
		public FinalizableQueryBuilder notStartsWithIgnoreCase(final String text) {
			checkNotNull(text, "Precondition violation - argument 'text' must not be NULL!");
			return this.addWhereDetails(Condition.NOT_STARTS_WITH, TextMatchMode.CASE_INSENSITIVE, text);
		}

		@Override
		public FinalizableQueryBuilder endsWith(final String text) {
			checkNotNull(text, "Precondition violation - argument 'text' must not be NULL!");
			return this.addWhereDetails(Condition.ENDS_WITH, TextMatchMode.STRICT, text);
		}

		@Override
		public FinalizableQueryBuilder endsWithIgnoreCase(final String text) {
			checkNotNull(text, "Precondition violation - argument 'text' must not be NULL!");
			return this.addWhereDetails(Condition.ENDS_WITH, TextMatchMode.CASE_INSENSITIVE, text);
		}

		@Override
		public FinalizableQueryBuilder notEndsWith(final String text) {
			checkNotNull(text, "Precondition violation - argument 'text' must not be NULL!");
			return this.addWhereDetails(Condition.NOT_ENDS_WITH, TextMatchMode.STRICT, text);
		}

		@Override
		public FinalizableQueryBuilder notEndsWithIgnoreCase(final String text) {
			checkNotNull(text, "Precondition violation - argument 'text' must not be NULL!");
			return this.addWhereDetails(Condition.NOT_ENDS_WITH, TextMatchMode.CASE_INSENSITIVE, text);
		}

		@Override
		public FinalizableQueryBuilder matchesRegex(final String regex) {
			checkNotNull(regex, "Precondition violation - argument 'regex' must not be NULL!");
			return this.addWhereDetails(Condition.MATCHES_REGEX, TextMatchMode.STRICT, regex);
		}

		@Override
		public FinalizableQueryBuilder notMatchesRegex(final String regex) {
			checkNotNull(regex, "Precondition violation - argument 'regex' must not be NULL!");
			return this.addWhereDetails(Condition.NOT_MATCHES_REGEX, TextMatchMode.STRICT, regex);
		}

		@Override
		public FinalizableQueryBuilder isEqualTo(final String value) {
			checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
			return this.addWhereDetails(Condition.EQUALS, TextMatchMode.STRICT, value);
		}

		@Override
		public FinalizableQueryBuilder isEqualToIgnoreCase(final String value) {
			checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
			return this.addWhereDetails(Condition.EQUALS, TextMatchMode.CASE_INSENSITIVE, value);
		}

		@Override
		public FinalizableQueryBuilder isNotEqualTo(final String value) {
			checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
			return this.addWhereDetails(Condition.NOT_EQUALS, TextMatchMode.STRICT, value);
		}

		@Override
		public FinalizableQueryBuilder isNotEqualToIgnoreCase(final String value) {
			checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
			return this.addWhereDetails(Condition.NOT_EQUALS, TextMatchMode.CASE_INSENSITIVE, value);
		}

		private FinalizableQueryBuilder addWhereDetails(final Condition condition, final TextMatchMode matchMode,
				final String text) {
			if (StandardQueryBuilder.this.currentWhereToken == null) {
				// this should never happen as such a query won't even compile in Java
				throw new ChronoDBQuerySyntaxException("Received 'contains', but no WHERE clause is open!");
			}
			StandardQueryBuilder.this.currentWhereToken.setCondition(condition);
			StandardQueryBuilder.this.currentWhereToken.setMatchMode(matchMode);
			StandardQueryBuilder.this.currentWhereToken.setComparisonValue(text);
			// we are done with that WHERE clause
			StandardQueryBuilder.this.currentWhereToken = null;
			return StandardQueryBuilder.this.finalizableBuilder;
		}
	}

	private class FinalizableQueryBuilderImpl extends AbstractFinalizableQueryBuilder
			implements FinalizableQueryBuilder {

		@Override
		protected ChronoDBQuery getQuery() {
			return this.toQuery();
		}

		@Override
		protected ChronoDBInternal getOwningDB() {
			return StandardQueryBuilder.this.owningDB;
		}

		@Override
		protected ChronoDBTransaction getTx() {
			return StandardQueryBuilder.this.tx;
		}

		@Override
		public ChronoDBQuery toQuery() {
			// add the End-Of-Input token to the stream
			QueryToken endOfInputToken = new EndOfInputToken();
			StandardQueryBuilder.this.tokenList.add(endOfInputToken);
			// parse the query
			ChronoDBQuery query = this.createOptimizedQuery();
			return query;
		}

		@Override
		public long count() {
			// add the End-Of-Input token to the stream
			QueryToken endOfInputToken = new EndOfInputToken();
			StandardQueryBuilder.this.tokenList.add(endOfInputToken);
			// parse the query
			ChronoDBQuery query = this.createOptimizedQuery();
			// evaluate the query
			String branchName = StandardQueryBuilder.this.tx.getBranchName();
			Branch branch = StandardQueryBuilder.this.owningDB.getBranchManager().getBranch(branchName);
			long timestamp = StandardQueryBuilder.this.tx.getTimestamp();
			return StandardQueryBuilder.this.owningDB.getIndexManager().evaluateCount(timestamp, branch, query);
		}

		@Override
		public QueryBuilder and() {
			QueryToken andToken = new AndToken();
			StandardQueryBuilder.this.tokenList.add(andToken);
			return StandardQueryBuilder.this.queryBuilder;
		}

		@Override
		public QueryBuilder or() {
			QueryToken orToken = new OrToken();
			StandardQueryBuilder.this.tokenList.add(orToken);
			return StandardQueryBuilder.this.queryBuilder;
		}

		@Override
		public FinalizableQueryBuilder begin() {
			QueryToken beginToken = new BeginToken();
			StandardQueryBuilder.this.tokenList.add(beginToken);
			return StandardQueryBuilder.this.finalizableBuilder;
		}

		@Override
		public FinalizableQueryBuilder end() {
			QueryToken endToken = new EndToken();
			StandardQueryBuilder.this.tokenList.add(endToken);
			return StandardQueryBuilder.this.finalizableBuilder;
		}

		@Override
		public FinalizableQueryBuilder not() {
			QueryToken notToken = new NotToken();
			StandardQueryBuilder.this.tokenList.add(notToken);
			return StandardQueryBuilder.this.finalizableBuilder;
		}

		private ChronoDBQuery createOptimizedQuery() {
			QueryTokenStream tokenStream = new StandardQueryTokenStream(StandardQueryBuilder.this.tokenList);
			QueryManager queryManager = StandardQueryBuilder.this.owningDB.getQueryManager();
			QueryParser parser = queryManager.getQueryParser();
			ChronoDBQuery query = parser.parse(tokenStream);
			QueryOptimizer optimizer = queryManager.getQueryOptimizer();
			return optimizer.optimize(query);
		}

	}

}
