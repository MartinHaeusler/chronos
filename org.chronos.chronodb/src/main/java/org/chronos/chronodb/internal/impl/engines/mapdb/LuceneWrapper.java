package org.chronos.chronodb.internal.impl.engines.mapdb;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.sandbox.queries.regex.RegexQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.chronos.chronodb.api.exceptions.ChronoDBQuerySyntaxException;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.api.query.searchspec.DoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.LongSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import com.google.common.collect.Iterables;

@SuppressWarnings("deprecation")
public class LuceneWrapper implements AutoCloseable {

	/** A java.util.Regex preceded with this prefix will be evaluated in case-insensitive mode. */
	private static final String REGEX_CI_CONSTRUCT = "(?i)";

	private final File ioDirectory;

	private final Directory directory;
	private final Analyzer analyzer;

	private final IndexWriter writer;

	private DirectoryReader reader;
	private IndexSearcher searcher;
	private final QueryParser queryParser;

	private boolean closed = false;

	private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

	public LuceneWrapper(final File directory) {
		checkNotNull(directory, "Precondition violation - argument 'directory' must not be NULL!");
		checkArgument(directory.isFile() == false,
				"Precondition violation - argument 'directory' does not refer to a directory (but a file)!");
		this.ioDirectory = directory;
		this.ioDirectory.mkdirs();
		this.ioDirectory.mkdir();
		if (this.ioDirectory.exists() == false || this.ioDirectory.isDirectory() == false) {
			throw new IllegalStateException(
					"Failed to initialize indexing directory '" + directory.getAbsolutePath() + "'!");
		}
		try {
			this.directory = FSDirectory.open(this.ioDirectory.toPath());
			this.analyzer = new StandardAnalyzer();
			IndexWriterConfig writerConfig = new IndexWriterConfig(this.analyzer);
			writerConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
			this.writer = new IndexWriter(this.directory, writerConfig);
			this.reader = DirectoryReader.open(this.writer, true);
			this.searcher = new IndexSearcher(this.reader);
			this.queryParser = new QueryParser(ChronoDBLuceneUtil.DOCUMENT_FIELD_ID, this.analyzer);
		} catch (IOException e) {
			throw new ChronoDBStorageBackendException(
					"Failed to initialize Lucene on directory '" + this.ioDirectory.getAbsolutePath() + "'!", e);
		}
	}

	// =================================================================================================================
	// CLOSE
	// =================================================================================================================

	@Override
	public void close() {
		if (this.closed) {
			return;
		}
		try {
			this.writer.close();
			this.reader.close();
			this.directory.close();
			this.analyzer.close();
			// lucene shutdown successful
			this.closed = true;
		} catch (IOException e) {
			throw new ChronoDBStorageBackendException("Failed to shutdown lucene wrapper!", e);
		}
	}

	public boolean isClosed() {
		return this.closed;
	}

	// =================================================================================================================
	// GENERIC SEARCH METHODS
	// =================================================================================================================

	public List<Document> search(final Query query) {
		checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
		this.assertNotClosed();
		this.rwLock.readLock().lock();
		try {
			AllDocumentsCollector collector = new AllDocumentsCollector();
			this.searcher.search(query, collector);
			return collector.getDocuments();
		} catch (IOException e) {
			throw new ChronoDBStorageBackendException("Failed to read index!", e);
		} finally {
			this.rwLock.readLock().unlock();
		}
	}

	public List<Document> search(final String searchString) {
		checkNotNull(searchString, "Precondition violation - argument 'searchString' must not be NULL!");
		this.assertNotClosed();
		this.rwLock.readLock().lock();
		try {
			Query query = this.queryParser.parse(searchString);
			return this.search(query);
		} catch (ParseException e) {
			throw new ChronoDBQuerySyntaxException("The syntax for the lucene query is invalid!", e);
		} finally {
			this.rwLock.readLock().unlock();
		}
	}

	// =================================================================================================================
	// CHRONO-SPECIFIC SEARCH METHODS
	// =================================================================================================================

	public Document getLuceneDocumentById(final String id) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		this.assertNotClosed();
		Query phraseQuery = new PhraseQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_ID, id);
		List<Document> documents = this.search(phraseQuery);
		return Iterables.getOnlyElement(documents);
	}

	public List<Document> getMatchingBranchLocalDocuments(final long timestamp, final String branchName,
			final String keyspace, final SearchSpecification<?> searchSpec) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		Query indexNameQuery = termQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEX_NAME, searchSpec.getProperty());
		Query branchQuery = termQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_BRANCH, branchName);
		Query keyspaceQuery = termQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_KEYSPACE, keyspace);
		// 0 <= validFrom <= timestamp
		Query validFromQuery = NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_VALID_FROM, 0L,
				timestamp, true, true);
		// timestamp < validTo < Long.MAX_VALUE
		Query validToQuery = NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_VALID_TO, timestamp,
				Long.MAX_VALUE, false, true);

		SearchSpecification<?> searchSpec2 = searchSpec;
		boolean isNegated = false;
		if (searchSpec.getCondition().isNegated()) {
			// remember that we need to negate the result and query for the non-negated spec
			isNegated = true;
			// create the non-negated spec
			searchSpec2 = searchSpec.negate();
		}
		Query searchSpecQuery = this.createSearchSpecQuery(searchSpec2);
		// build the composite query
		Builder queryBuilder = new Builder();
		queryBuilder.add(indexNameQuery, Occur.FILTER);
		queryBuilder.add(branchQuery, Occur.FILTER);
		queryBuilder.add(keyspaceQuery, Occur.FILTER);
		queryBuilder.add(validFromQuery, Occur.FILTER);
		queryBuilder.add(validToQuery, Occur.FILTER);
		if (isNegated) {
			queryBuilder.add(searchSpecQuery, Occur.MUST_NOT);
		} else {
			queryBuilder.add(searchSpecQuery, Occur.FILTER);
		}
		Query query = queryBuilder.build();
		return this.search(query);
	}

	public List<Document> getMatchingBranchLocalDocuments(final ChronoIdentifier chronoIdentifier) {
		Query branchQuery = termQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_BRANCH, chronoIdentifier.getBranchName());
		Query keyspaceQuery = termQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_KEYSPACE, chronoIdentifier.getKeyspace());
		Query keyQuery = termQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_KEY, chronoIdentifier.getKey());
		// 0 <= validFrom <= timestamp
		Query validFromQuery = NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_VALID_FROM, 0L,
				chronoIdentifier.getTimestamp(), true, true);
		// timestamp < validTo < Long.MAX_VALUE
		Query validToQuery = NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_VALID_TO,
				chronoIdentifier.getTimestamp(), Long.MAX_VALUE, false, true);
		// build the composite query
		Builder queryBuilder = new Builder();
		queryBuilder.add(branchQuery, Occur.FILTER);
		queryBuilder.add(keyspaceQuery, Occur.FILTER);
		queryBuilder.add(keyQuery, Occur.FILTER);
		queryBuilder.add(validFromQuery, Occur.FILTER);
		queryBuilder.add(validToQuery, Occur.FILTER);
		Query compositeQuery = queryBuilder.build();
		return this.search(compositeQuery);
	}

	public Collection<Document> getTerminatedBranchLocalDocuments(final long timestamp, final String branchName,
			final String keyspace, final SearchSpecification<?> searchSpec) {
		Query indexNameQuery = termQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEX_NAME, searchSpec.getProperty());
		Query branchQuery = termQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_BRANCH, branchName);
		Query keyspaceQuery = termQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_KEYSPACE, keyspace);
		// 0 <= validTo <= timestamp
		Query validToQuery = NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_VALID_TO, 0L, timestamp,
				true, true);
		Query searchSpecQuery = this.createSearchSpecQuery(searchSpec);
		// build the composite query
		Builder queryBuilder = new Builder();
		queryBuilder.add(indexNameQuery, Occur.FILTER);
		queryBuilder.add(branchQuery, Occur.FILTER);
		queryBuilder.add(keyspaceQuery, Occur.FILTER);
		queryBuilder.add(validToQuery, Occur.FILTER);
		queryBuilder.add(searchSpecQuery, Occur.FILTER);
		return this.search(queryBuilder.build());
	}

	// =================================================================================================================
	// INDEX MODIFICATION
	// =================================================================================================================

	public void performIndexWrite(final NonReturningIndexWriteJob job) {
		checkNotNull(job, "Precondition violation - argument 'job' must not be NULL!");
		this.assertNotClosed();
		try {
			job.execute(this.writer);
			this.writer.commit();
			// note: we do this in the "write" method, in order to not burden a read-task with
			// the performance loss.
			this.refreshIndexReader();
		} catch (IOException e) {
			throw new ChronoDBStorageBackendException("Failed to perform index modification!", e);
		}
	}

	public <T> T performIndexWrite(final ReturningIndexWriteJob<T> job) {
		checkNotNull(job, "Precondition violation - argument 'job' must not be NULL!");
		this.assertNotClosed();
		try {
			T result = job.execute(this.writer);
			this.writer.commit();
			// note: we do this in the "write" method, in order to not burden a read-task with
			// the performance loss.
			this.refreshIndexReader();
			return result;
		} catch (IOException e) {
			throw new ChronoDBStorageBackendException("Failed to perform index modification!", e);
		}
	}

	public void deleteAllDocuments() {
		this.performIndexWrite(indexWriter -> {
			indexWriter.deleteAll();
		});
	}

	public void deleteDocumentsByIndexName(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		this.performIndexWrite(indexWriter -> {
			indexWriter.deleteDocuments(new Term(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEX_NAME, indexName));
		});
	}

	public List<Document> getDocumentsTouchedAtOrAfterTimestamp(final long timestamp, final Set<String> branches) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(branches, "Precondition violation - argument 'branches' must not be NULL!");
		// validFrom >= timestamp <= Long.MAX_VALUE (infinity)
		Query validFromQuery = NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_VALID_FROM, timestamp,
				Long.MAX_VALUE, true, true);
		// validTo >= timestamp <= Long.MAX_VALUE (infinity)
		Query validToQuery = NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_VALID_TO, timestamp,
				Long.MAX_VALUE, true, true);
		// build the composite query
		Builder queryBuilder = new Builder();
		queryBuilder.add(validFromQuery, Occur.SHOULD);
		queryBuilder.add(validToQuery, Occur.SHOULD);
		List<Document> docs = this.search(queryBuilder.build());
		// we post-process the "in" clause here in-memory, this isn't ideal, perhaps there's a better way to do it
		// directly in lucene?
		docs = docs.stream().filter(doc -> branches.contains(doc.get(ChronoDBLuceneUtil.DOCUMENT_FIELD_BRANCH)))
				.collect(Collectors.toList());
		return docs;
	}

	// =================================================================================================================
	// INTERNAL UTILITY METHODS
	// =================================================================================================================

	private void assertNotClosed() {
		if (this.isClosed()) {
			throw new IllegalStateException("This lucene wrapper is already closed!");
		}
	}

	private void refreshIndexReader() {
		try {
			DirectoryReader newReader = DirectoryReader.openIfChanged(this.reader);
			if (newReader != null) {
				this.rwLock.writeLock().lock();
				try {
					// a new reader is created, so the directory has changed.
					// discard the old reader
					this.searcher = null;
					this.reader.close();
					this.reader = newReader;
					this.searcher = new IndexSearcher(newReader);
				} finally {
					this.rwLock.writeLock().unlock();
				}
			}
		} catch (IOException e) {
			throw new ChronoDBStorageBackendException("Failed to refresh index reader!", e);
		}
	}

	private String preprocessString(final String comparisonValue, final TextMatchMode mode) {
		switch (mode) {
		case STRICT:
			return comparisonValue;
		case CASE_INSENSITIVE:
			return comparisonValue.toLowerCase();
		default:
			throw new UnknownEnumLiteralException(mode);
		}
	}

	private static Query termQuery(final String field, final String text) {
		return new TermQuery(new Term(field, text));
	}

	private Query createSearchSpecQuery(final SearchSpecification<?> searchSpec) {
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		if (searchSpec instanceof StringSearchSpecification) {
			return this.createStringSearchSpecQuery((StringSearchSpecification) searchSpec);
		} else if (searchSpec instanceof LongSearchSpecification) {
			return this.createLongSearchSpecQuery((LongSearchSpecification) searchSpec);
		} else if (searchSpec instanceof DoubleSearchSpecification) {
			return this.createDoubleSearchSpecQuery((DoubleSearchSpecification) searchSpec);
		} else {
			throw new IllegalStateException("Unknown search specification class: '" + searchSpec.getClass().getName() + "'!");
		}
	}

	private Query createStringSearchSpecQuery(final StringSearchSpecification searchSpec) {
		StringCondition condition = searchSpec.getCondition();
		TextMatchMode matchMode = searchSpec.getMatchMode();
		String comparisonValue = searchSpec.getSearchValue();
		// note: negated conditions (e.g. NOT_EQUALS) are handled on a higher level.
		if (condition.equals(StringCondition.EQUALS)) {
			return this.createStringEqualsQuery(comparisonValue, matchMode);
		} else if (condition.equals(StringCondition.CONTAINS)) {
			return this.createStringContainsQuery(comparisonValue, matchMode);
		} else if (condition.equals(StringCondition.STARTS_WITH)) {
			return this.createStringStartsWithQuery(comparisonValue, matchMode);
		} else if (condition.equals(StringCondition.ENDS_WITH)) {
			return this.createStringEndsWithQuery(comparisonValue, matchMode);
		} else if (condition.equals(StringCondition.MATCHES_REGEX)) {
			// in this case, the comparison value contains the regex to match
			return this.createStringRegexQuery(comparisonValue, matchMode);
		} else {
			throw new IllegalStateException("Unknown StringCondition: '" + condition.getClass().getName() + "'!");
		}
	}

	private Query createLongSearchSpecQuery(final LongSearchSpecification searchSpec) {
		NumberCondition condition = searchSpec.getCondition();
		long comparisonValue = searchSpec.getSearchValue();
		// note: negated conditions (e.g. NOT_EQUALS) are handled on a higher level.
		if (condition.equals(NumberCondition.EQUALS)) {
			return this.createLongEqualsQuery(comparisonValue);
		} else if (condition.equals(NumberCondition.GREATER_THAN)) {
			return this.createLongGreaterThanQuery(comparisonValue);
		} else if (condition.equals(NumberCondition.GREATER_EQUAL)) {
			return this.createLongGreaterEqualQuery(comparisonValue);
		} else if (condition.equals(NumberCondition.LESS_THAN)) {
			return this.createLongLessThanQuery(comparisonValue);
		} else if (condition.equals(NumberCondition.LESS_EQUAL)) {
			return this.createLongLessEqualQuery(comparisonValue);
		} else {
			throw new IllegalStateException("Unknown NumberCondition: '" + condition.getClass().getName() + "'!");
		}
	}

	private Query createDoubleSearchSpecQuery(final DoubleSearchSpecification searchSpec) {
		NumberCondition condition = searchSpec.getCondition();
		double comparisonValue = searchSpec.getSearchValue();
		double equalityTolerance = searchSpec.getEqualityTolerance();
		// note: negated conditions (e.g. NOT_EQUALS) are handled on a higher level.
		if (condition.equals(NumberCondition.EQUALS)) {
			return this.createDoubleEqualsQuery(comparisonValue, equalityTolerance);
		} else if (condition.equals(NumberCondition.GREATER_THAN)) {
			return this.createDoubleGreaterThanQuery(comparisonValue);
		} else if (condition.equals(NumberCondition.GREATER_EQUAL)) {
			return this.createDoubleGreaterEqualQuery(comparisonValue);
		} else if (condition.equals(NumberCondition.LESS_THAN)) {
			return this.createDoubleLessThanQuery(comparisonValue);
		} else if (condition.equals(NumberCondition.LESS_EQUAL)) {
			return this.createDoubleLessEqualQuery(comparisonValue);
		} else {
			throw new IllegalStateException("Unknown NumberCondition: '" + condition.getClass().getName() + "'!");
		}
	}

	private Query createStringEqualsQuery(final String comparisonValue, final TextMatchMode matchMode) {
		String field = null;
		switch (matchMode) {
		case STRICT:
			field = ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE;
			break;
		case CASE_INSENSITIVE:
			field = ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE_CI;
			break;
		default:
			throw new UnknownEnumLiteralException(matchMode);
		}
		return termQuery(field, this.preprocessString(comparisonValue, matchMode));
	}

	private Query createStringContainsQuery(final String comparisonValue, final TextMatchMode matchMode) {
		String regex = ".*" + Pattern.quote(this.preprocessString(comparisonValue, matchMode)) + ".*";
		return this.createStringRegexQuery(regex, matchMode);
	}

	private Query createStringStartsWithQuery(final String comparisonValue, final TextMatchMode matchMode) {
		String regex = Pattern.quote(this.preprocessString(comparisonValue, matchMode)) + ".*";
		return this.createStringRegexQuery(regex, matchMode);
	}

	private Query createStringEndsWithQuery(final String comparisonValue, final TextMatchMode matchMode) {
		String regex = ".*" + Pattern.quote(this.preprocessString(comparisonValue, matchMode));
		return this.createStringRegexQuery(regex, matchMode);
	}

	private Query createStringRegexQuery(final String regex, final TextMatchMode matchMode) {
		String field = null;
		String expression = regex;
		switch (matchMode) {
		case STRICT:
			field = ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE;
			break;
		case CASE_INSENSITIVE:
			field = ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE_CI;
			if (expression.startsWith(REGEX_CI_CONSTRUCT) == false) {
				expression = REGEX_CI_CONSTRUCT + expression;
			}
			break;
		default:
			throw new UnknownEnumLiteralException(matchMode);
		}
		return new RegexQuery(new Term(field, expression));
	}

	private Query createLongEqualsQuery(final long comparisonValue) {
		return NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE_LONG, comparisonValue, comparisonValue, true, true);
	}

	private Query createLongGreaterThanQuery(final long comparisonValue) {
		return NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE_LONG, comparisonValue, Long.MAX_VALUE, false, true);
	}

	private Query createLongGreaterEqualQuery(final long comparisonValue) {
		return NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE_LONG, comparisonValue, Long.MAX_VALUE, true, true);
	}

	private Query createLongLessThanQuery(final long comparisonValue) {
		return NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE_LONG, Long.MIN_VALUE, comparisonValue, true, false);
	}

	private Query createLongLessEqualQuery(final long comparisonValue) {
		return NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE_LONG, Long.MIN_VALUE, comparisonValue, true, true);
	}

	private Query createDoubleEqualsQuery(final double comparisonValue, final double equalityTolerance) {
		return NumericRangeQuery.newDoubleRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE_DOUBLE, comparisonValue - equalityTolerance, comparisonValue + equalityTolerance, true, true);
	}

	private Query createDoubleGreaterThanQuery(final double comparisonValue) {
		return NumericRangeQuery.newDoubleRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE_DOUBLE, comparisonValue, Double.MAX_VALUE, false, true);
	}

	private Query createDoubleGreaterEqualQuery(final double comparisonValue) {
		return NumericRangeQuery.newDoubleRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE_DOUBLE, comparisonValue, Double.MAX_VALUE, true, true);
	}

	private Query createDoubleLessThanQuery(final double comparisonValue) {
		return NumericRangeQuery.newDoubleRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE_DOUBLE, Double.MIN_VALUE, comparisonValue, true, false);
	}

	private Query createDoubleLessEqualQuery(final double comparisonValue) {
		return NumericRangeQuery.newDoubleRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEXED_VALUE_DOUBLE, Double.MIN_VALUE, comparisonValue, true, true);
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	public interface NonReturningIndexWriteJob {

		public void execute(IndexWriter writer) throws IOException;

	}

	public interface ReturningIndexWriteJob<T> {

		public T execute(IndexWriter writer) throws IOException;

	}

}
