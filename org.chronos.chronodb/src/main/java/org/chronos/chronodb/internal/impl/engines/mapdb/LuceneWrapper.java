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
import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.query.SearchSpecification;
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
		if ((this.ioDirectory.exists() == false) || (this.ioDirectory.isDirectory() == false)) {
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
			final SearchSpecification searchSpec) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		Query indexNameQuery = termQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEX_NAME, searchSpec.getProperty());
		Query branchQuery = termQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_BRANCH, branchName);
		// 0 <= validFrom <= timestamp
		Query validFromQuery = NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_VALID_FROM, 0L,
				timestamp, true, true);
		// timestamp < validTo < Long.MAX_VALUE
		Query validToQuery = NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_VALID_TO, timestamp,
				Long.MAX_VALUE, false, true);

		SearchSpecification searchSpec2 = searchSpec;
		boolean isNegated = false;
		if (searchSpec.getCondition().isNegated()) {
			// remember that we need to negate the result and query for the non-negated spec
			isNegated = true;
			// create the non-negated spec
			searchSpec2 = SearchSpecification.create(searchSpec.getProperty(), searchSpec.getCondition().getNegated(),
					searchSpec.getMatchMode(), searchSpec.getSearchText());
		}
		Query searchSpecQuery = this.createSearchSpecQuery(searchSpec2);
		// build the composite query
		Builder queryBuilder = new Builder();
		queryBuilder.add(indexNameQuery, Occur.FILTER);
		queryBuilder.add(branchQuery, Occur.FILTER);
		queryBuilder.add(validFromQuery, Occur.FILTER);
		queryBuilder.add(validToQuery, Occur.FILTER);
		if (isNegated) {
			queryBuilder.add(searchSpecQuery, Occur.MUST_NOT);
		} else {
			queryBuilder.add(searchSpecQuery, Occur.FILTER);
		}
		return this.search(queryBuilder.build());
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
			final SearchSpecification searchSpec) {
		Query indexNameQuery = termQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_INDEX_NAME, searchSpec.getProperty());
		Query branchQuery = termQuery(ChronoDBLuceneUtil.DOCUMENT_FIELD_BRANCH, branchName);
		// 0 <= validTo <= timestamp
		Query validToQuery = NumericRangeQuery.newLongRange(ChronoDBLuceneUtil.DOCUMENT_FIELD_VALID_TO, 0L, timestamp,
				true, true);
		Query searchSpecQuery = this.createSearchSpecQuery(searchSpec);
		// build the composite query
		Builder queryBuilder = new Builder();
		queryBuilder.add(indexNameQuery, Occur.FILTER);
		queryBuilder.add(branchQuery, Occur.FILTER);
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

	private Query createSearchSpecQuery(final SearchSpecification searchSpec) {
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		Condition condition = searchSpec.getCondition();
		TextMatchMode matchMode = searchSpec.getMatchMode();
		String comparisonValue = searchSpec.getSearchText();
		switch (condition) {
		case EQUALS:
			return this.createEqualsQuery(comparisonValue, matchMode);
		case NOT_EQUALS:
			return this.createNotEqualsQuery(comparisonValue, matchMode);
		case CONTAINS:
			return this.createContainsQuery(comparisonValue, matchMode);
		case NOT_CONTAINS:
			return this.createNotContainsQuery(comparisonValue, matchMode);
		case STARTS_WITH:
			return this.createStartsWithQuery(comparisonValue, matchMode);
		case NOT_STARTS_WITH:
			return this.createNotStartsWithQuery(comparisonValue, matchMode);
		case ENDS_WITH:
			return this.createEndsWithQuery(comparisonValue, matchMode);
		case NOT_ENDS_WITH:
			return this.createNotEndsWithQuery(comparisonValue, matchMode);
		case MATCHES_REGEX:
			// in this case, the comparison value contains the regex to match
			return this.createRegexQuery(comparisonValue, matchMode);
		case NOT_MATCHES_REGEX:
			// in this case, the comparison value contains the regex to match
			return this.createNegatedRegexQuery(comparisonValue, matchMode);
		default:
			throw new UnknownEnumLiteralException(condition);
		}
	}

	private Query createEqualsQuery(final String comparisonValue, final TextMatchMode matchMode) {
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

	private Query createNotEqualsQuery(final String comparisonValue, final TextMatchMode matchMode) {
		Builder queryBuilder = new Builder();
		queryBuilder.add(this.createEqualsQuery(comparisonValue, matchMode), Occur.MUST_NOT);
		return queryBuilder.build();
	}

	private Query createContainsQuery(final String comparisonValue, final TextMatchMode matchMode) {
		String regex = ".*" + Pattern.quote(this.preprocessString(comparisonValue, matchMode)) + ".*";
		return this.createRegexQuery(regex, matchMode);
	}

	private Query createNotContainsQuery(final String comparisonValue, final TextMatchMode matchMode) {
		String regex = ".*" + Pattern.quote(this.preprocessString(comparisonValue, matchMode)) + ".*";
		return this.createNegatedRegexQuery(regex, matchMode);
	}

	private Query createStartsWithQuery(final String comparisonValue, final TextMatchMode matchMode) {
		String regex = Pattern.quote(this.preprocessString(comparisonValue, matchMode)) + ".*";
		return this.createRegexQuery(regex, matchMode);
	}

	private Query createNotStartsWithQuery(final String comparisonValue, final TextMatchMode matchMode) {
		String regex = Pattern.quote(this.preprocessString(comparisonValue, matchMode)) + ".*";
		return this.createNegatedRegexQuery(regex, matchMode);
	}

	private Query createEndsWithQuery(final String comparisonValue, final TextMatchMode matchMode) {
		String regex = ".*" + Pattern.quote(this.preprocessString(comparisonValue, matchMode));
		return this.createRegexQuery(regex, matchMode);
	}

	private Query createNotEndsWithQuery(final String comparisonValue, final TextMatchMode matchMode) {
		String regex = ".*" + Pattern.quote(this.preprocessString(comparisonValue, matchMode));
		return this.createNegatedRegexQuery(regex, matchMode);
	}

	private Query createRegexQuery(final String regex, final TextMatchMode matchMode) {
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

	private Query createNegatedRegexQuery(final String regex, final TextMatchMode matchMode) {
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
