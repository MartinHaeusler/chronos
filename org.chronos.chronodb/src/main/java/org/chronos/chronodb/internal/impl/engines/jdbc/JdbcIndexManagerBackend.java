package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.api.exceptions.JdbcTableException;
import org.chronos.chronodb.api.exceptions.UnknownIndexException;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.api.index.ChronoIndexModifications;
import org.chronos.chronodb.internal.api.index.DocumentAddition;
import org.chronos.chronodb.internal.api.index.DocumentDeletion;
import org.chronos.chronodb.internal.api.index.DocumentValidityTermination;
import org.chronos.chronodb.internal.api.query.searchspec.DoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.LongSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification;
import org.chronos.chronodb.internal.impl.engines.base.AbstractDocumentBasedIndexManagerBackend;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.util.ReflectionUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class JdbcIndexManagerBackend extends AbstractDocumentBasedIndexManagerBackend {

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public JdbcIndexManagerBackend(final JdbcChronoDB owningDB) {
		super(owningDB);
		this.ensureJdbcTablesExist();
	}

	// =================================================================================================================
	// INDEXER MANAGEMENT
	// =================================================================================================================

	@Override
	public SetMultimap<String, Indexer<?>> loadIndexersFromPersistence() {
		try (Connection connection = this.openConnection()) {
			return JdbcIndexerTable.get(connection).getIndexers();
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Unable to load indexers from database!", e);
		}
	}

	@Override
	public void persistIndexers(final SetMultimap<String, Indexer<?>> indexNameToIndexers) {
		checkNotNull(indexNameToIndexers, "Precondition violation - argument 'indexNameToIndexers' must not be NULL!");
		try (Connection connection = this.openConnection()) {
			connection.setAutoCommit(false);
			JdbcIndexerTable indexerTable = JdbcIndexerTable.get(connection);
			for (String indexName : indexNameToIndexers.keySet()) {
				// drop the indexers in the table that belong to the index, such that we can simply INSERT them later
				// TODO PERFORMANCE JDBC: dropping all and re-inserting to avoid UPDATE is not very efficient.
				indexerTable.removeAllIndexersOfIndex(indexName);
			}
			for (Entry<String, Collection<Indexer<?>>> entry : indexNameToIndexers.asMap().entrySet()) {
				String indexName = entry.getKey();
				Collection<Indexer<?>> indexers = entry.getValue();
				for (Indexer<?> indexer : indexers) {
					indexerTable.insert(indexName, indexer);
				}
			}
			connection.commit();
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Unable to persist indexers to database!", e);
		}
	}

	@Override
	public void deleteIndexAndIndexers(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		try (Connection connection = this.openConnection()) {
			JdbcIndexerTable.get(connection).removeAllIndexersOfIndex(indexName);
			JdbcIndexDirtyFlagsTable.get(connection).removeIndexDirtyFlag(indexName);
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Unable to delete indexers from database!", e);
		}
	}

	@Override
	public void deleteAllIndicesAndIndexers() {
		try (Connection connection = this.openConnection()) {
			JdbcIndexerTable.get(connection).removeAllIndexers();
			JdbcIndexDirtyFlagsTable.get(connection).removeIndexDirtyFlags();
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Unable to delete indexers from database!", e);
		}
	}

	@Override
	public void deleteIndexContents(final String indexName) {
		try (Connection connection = this.openConnection()) {
			JdbcIndexerTable.get(connection).removeAllIndexersOfIndex(indexName);
			JdbcIndexDirtyFlagsTable.get(connection).removeIndexDirtyFlag(indexName);
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Unable to delete indices from database!", e);
		}
	}

	@Override
	public void persistIndexer(final String indexName, final Indexer<?> indexer) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(indexer, "Precondition violation - argument 'indexer' must not be NULL!");
		try (Connection connection = this.openConnection()) {
			JdbcIndexerTable.get(connection).insert(indexName, indexer);
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Unable to insert indexers into database!", e);
		}
	}

	// =================================================================================================================
	// INDEX DIRTY FLAG MANAGEMENT
	// =================================================================================================================

	@Override
	public Map<String, Boolean> loadIndexStates() {
		try (Connection connection = this.openConnection()) {
			return JdbcIndexDirtyFlagsTable.get(connection).getIndexStates();
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Unable to load index metadata from database!", e);
		}
	}

	@Override
	public void persistIndexDirtyStates(final Map<String, Boolean> indices) {
		checkNotNull(indices, "Precondition violation - argument 'indices' must not be NULL!");
		try (Connection connection = this.openConnection()) {
			connection.setAutoCommit(false);
			JdbcIndexDirtyFlagsTable table = JdbcIndexDirtyFlagsTable.get(connection);
			// TODO PERFORMANCE JDBC: Dropping all entries and re-inserting is safe but not very efficient
			table.removeIndexDirtyFlags();
			// perform the inserts according to the given set
			for (Entry<String, Boolean> entry : indices.entrySet()) {
				String indexName = entry.getKey();
				boolean dirtyFlag = entry.getValue();
				table.insert(indexName, dirtyFlag);
			}
			connection.commit();
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Unable to update index metadata in database!", e);
		}
	}

	// =================================================================================================================
	// INDEX DOCUMENT MANAGEMENT
	// =================================================================================================================

	@Override
	public void deleteAllIndexContents() {
		try (Connection connection = this.openConnection()) {
			JdbcStringIndexDocumentTable documentsTable = JdbcStringIndexDocumentTable.get(connection);
			documentsTable.drop();
			documentsTable.create();
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Failed to query Index Documents Table!", e);
		}
	}

	@Override
	public void applyModifications(final ChronoIndexModifications indexModifications) {
		checkNotNull(indexModifications, "Precondition violation - argument 'indexModifications' must not be NULL!");
		if (indexModifications.isEmpty()) {
			return;
		}
		try (Connection connection = this.openConnection()) {
			ChronoLogger.logDebug("Applying index modifications: " + indexModifications);
			for (DocumentValidityTermination termination : indexModifications.getDocumentValidityTerminations()) {
				ChronoIndexDocument document = termination.getDocument();
				long timestamp = termination.getTerminationTimestamp();
				this.terminateDocumentValidity(connection, document, timestamp);
			}
			for (DocumentAddition creation : indexModifications.getDocumentCreations()) {
				ChronoIndexDocument document = creation.getDocumentToAdd();
				this.persistDocument(connection, document);
			}
			for (DocumentDeletion deletion : indexModifications.getDocumentDeletions()) {
				this.deleteIndexDocument(connection, deletion.getDocumentToDelete());
			}
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Could not apply index modifications!", e);
		}
	}

	@Override
	protected Set<ChronoIndexDocument> getDocumentsTouchedAtOrAfterTimestamp(final long timestamp, final Set<String> branches) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(branches, "Precondition violation - argument 'branches' must not be NULL!");
		Set<ChronoIndexDocument> resultSet = Sets.newHashSet();
		if (branches.isEmpty()) {
			return resultSet;
		}
		try (Connection connection = this.openConnection()) {
			JdbcStringIndexDocumentTable documentsTable = JdbcStringIndexDocumentTable.get(connection);
			resultSet.addAll(documentsTable.getDocumentsTouchedAtOrAfterTimestamp(timestamp, branches));
			JdbcLongIndexDocumentTable longDocumentsTable = JdbcLongIndexDocumentTable.get(connection);
			resultSet.addAll(longDocumentsTable.getDocumentsTouchedAtOrAfterTimestamp(timestamp, branches));
			JdbcDoubleIndexDocumentTable doubleDocumentsTable = JdbcDoubleIndexDocumentTable.get(connection);
			resultSet.addAll(doubleDocumentsTable.getDocumentsTouchedAtOrAfterTimestamp(timestamp, branches));
			return resultSet;
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Failed to query Index Documents Table!", e);
		}
	}

	// =================================================================================================================
	// INDEX QUERYING
	// =================================================================================================================

	@Override
	public Map<String, SetMultimap<Object, ChronoIndexDocument>> getMatchingBranchLocalDocuments(final ChronoIdentifier chronoIdentifier) {
		checkNotNull(chronoIdentifier, "Precondition violation - argument 'chronoIdentifier' must not be NULL!");
		Set<ChronoIndexDocument> documents = Sets.newHashSet();
		try (Connection connection = this.openConnection()) {
			documents.addAll(JdbcStringIndexDocumentTable.get(connection).getMatchingBranchLocalDocuments(chronoIdentifier));
			documents.addAll(JdbcLongIndexDocumentTable.get(connection).getMatchingBranchLocalDocuments(chronoIdentifier));
			documents.addAll(JdbcDoubleIndexDocumentTable.get(connection).getMatchingBranchLocalDocuments(chronoIdentifier));
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Failed to query Index Documents Table!", e);
		}
		// sort the resulting documents into the required structure and convert lucene docs to chrono index docs
		Map<String, SetMultimap<Object, ChronoIndexDocument>> resultMap = Maps.newHashMap();
		for (ChronoIndexDocument document : documents) {
			String indexName = document.getIndexName();
			SetMultimap<Object, ChronoIndexDocument> indexValueToDocument = resultMap.get(indexName);
			if (indexValueToDocument == null) {
				indexValueToDocument = HashMultimap.create();
				resultMap.put(indexName, indexValueToDocument);
			}
			indexValueToDocument.put(document.getIndexedValue(), document);
		}
		return resultMap;
	}

	@Override
	protected Collection<ChronoIndexDocument> getTerminatedBranchLocalDocuments(final long timestamp, final String branchName, final String keyspace, final SearchSpecification<?> searchSpec) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		return this.performSearchInternal(branchName, keyspace, timestamp, TimeSearchMode.TERMINATED_AT_OR_BEFORE_TIMESTAMP, searchSpec);
	}

	@Override
	protected Collection<ChronoIndexDocument> getMatchingBranchLocalDocuments(final long timestamp, final String branchName, final String keyspace, final SearchSpecification<?> searchSpec) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		return this.performSearchInternal(branchName, keyspace, timestamp, TimeSearchMode.VALID_AT_TIMESTAMP, searchSpec);
	}

	private Set<ChronoIndexDocument> performSearchInternal(final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final SearchSpecification<?> searchSpec) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(timeSearchMode, "Precondition violation - argument 'timeSearchMode' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		if (searchSpec instanceof StringSearchSpecification) {
			return this.performSearchInternal(branchName, keyspace, timestamp, timeSearchMode, (StringSearchSpecification) searchSpec);
		} else if (searchSpec instanceof LongSearchSpecification) {
			return this.performSearchInternal(branchName, keyspace, timestamp, timeSearchMode, (LongSearchSpecification) searchSpec);
		} else if (searchSpec instanceof DoubleSearchSpecification) {
			return this.performSearchInternal(branchName, keyspace, timestamp, timeSearchMode, (DoubleSearchSpecification) searchSpec);
		} else {
			throw new IllegalStateException("Unknown search specification class: '" + searchSpec.getClass().getName() + "'!");
		}
	}

	private Set<ChronoIndexDocument> performSearchInternal(final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final StringSearchSpecification searchSpec) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(timeSearchMode, "Precondition violation - argument 'timeSearchMode' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		try (Connection connection = this.openConnection()) {
			JdbcStringIndexDocumentTable documentsTable = JdbcStringIndexDocumentTable.get(connection);
			JdbcIndexDirtyFlagsTable indexDirtyFlagsTable = JdbcIndexDirtyFlagsTable.get(connection);
			String indexName = searchSpec.getProperty();
			Boolean indexState = indexDirtyFlagsTable.isIndexDirty(indexName);
			if (indexState == null) {
				// index does not exist!
				throw new UnknownIndexException("There is no index named '" + indexName + "'!");
			}
			// we need to do a case distinction based on the condition at hand, because some of the
			// conditions are natively supported by SQL, others need to be emulated.
			StringCondition condition = searchSpec.getCondition();
			String comparisonValue = searchSpec.getSearchValue();
			TextMatchMode matchMode = searchSpec.getMatchMode();
			if (condition.equals(StringCondition.CONTAINS)) {
				return this.getMatchingDocumentsContains(documentsTable, indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue, matchMode);
			} else if (condition.equals(StringCondition.ENDS_WITH)) {
				return this.getMatchingDocumentsEndsWith(documentsTable, indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue, matchMode);
			} else if (condition.equals(StringCondition.EQUALS)) {
				return this.getMatchingDocumentsEquals(documentsTable, indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue, matchMode);
			} else if (condition.equals(StringCondition.MATCHES_REGEX)) {
				return this.getMatchingDocumentsMatchesRegex(documentsTable, indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue, matchMode);
			} else if (condition.equals(StringCondition.NOT_CONTAINS)) {
				return this.getMatchingDocumentsNotContains(documentsTable, indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue, matchMode);
			} else if (condition.equals(StringCondition.NOT_ENDS_WITH)) {
				return this.getMatchingDocumentsNotEndsWith(documentsTable, indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue, matchMode);
			} else if (condition.equals(StringCondition.NOT_EQUALS)) {
				return this.getMatchingDocumentsNotEquals(documentsTable, indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue, matchMode);
			} else if (condition.equals(StringCondition.NOT_MATCHES_REGEX)) {
				return this.getMatchingDocumentsNotMatchesRegex(documentsTable, indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue, matchMode);
			} else if (condition.equals(StringCondition.NOT_STARTS_WITH)) {
				return this.getMatchingDocumentsNotStartsWith(documentsTable, indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue, matchMode);
			} else if (condition.equals(StringCondition.STARTS_WITH)) {
				return this.getMatchingDocumentsStartsWith(documentsTable, indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue, matchMode);
			} else {
				throw new IllegalStateException("Unknown StringCondition: '" + condition.getClass().getName() + "'!");
			}
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Could not query Index Documents Table!", e);
		}
	}

	private Set<ChronoIndexDocument> performSearchInternal(final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final LongSearchSpecification searchSpec) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(timeSearchMode, "Precondition violation - argument 'timeSearchMode' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		try (Connection connection = this.openConnection()) {
			JdbcLongIndexDocumentTable documentsTable = JdbcLongIndexDocumentTable.get(connection);
			JdbcIndexDirtyFlagsTable indexDirtyFlagsTable = JdbcIndexDirtyFlagsTable.get(connection);
			String indexName = searchSpec.getProperty();
			Boolean indexState = indexDirtyFlagsTable.isIndexDirty(indexName);
			if (indexState == null) {
				// index does not exist!
				throw new UnknownIndexException("There is no index named '" + indexName + "'!");
			}
			NumberCondition condition = searchSpec.getCondition();
			long comparisonValue = searchSpec.getSearchValue();
			if (condition.equals(NumberCondition.EQUALS)) {
				return documentsTable.getDocumentsWhereValueEquals(indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue);
			} else if (condition.equals(NumberCondition.NOT_EQUALS)) {
				return documentsTable.getDocumentsWhereValueNotEquals(indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue);
			} else if (condition.equals(NumberCondition.GREATER_THAN)) {
				return documentsTable.getDocumentsWhereValueIsGreaterThan(indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue);
			} else if (condition.equals(NumberCondition.GREATER_EQUAL)) {
				return documentsTable.getDocumentsWhereValueIsGreaterOrEqual(indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue);
			} else if (condition.equals(NumberCondition.LESS_THAN)) {
				return documentsTable.getDocumentsWhereValueIsLessThan(indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue);
			} else if (condition.equals(NumberCondition.LESS_EQUAL)) {
				return documentsTable.getDocumentsWhereValueIsLessOrEqual(indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue);
			} else {
				throw new IllegalStateException("Unknown StringCondition: '" + condition.getClass().getName() + "'!");
			}
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Could not query Index Documents Table!", e);
		}
	}

	private Set<ChronoIndexDocument> performSearchInternal(final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final DoubleSearchSpecification searchSpec) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(timeSearchMode, "Precondition violation - argument 'timeSearchMode' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		try (Connection connection = this.openConnection()) {
			JdbcDoubleIndexDocumentTable documentsTable = JdbcDoubleIndexDocumentTable.get(connection);
			JdbcIndexDirtyFlagsTable indexDirtyFlagsTable = JdbcIndexDirtyFlagsTable.get(connection);
			String indexName = searchSpec.getProperty();
			Boolean indexState = indexDirtyFlagsTable.isIndexDirty(indexName);
			if (indexState == null) {
				// index does not exist!
				throw new UnknownIndexException("There is no index named '" + indexName + "'!");
			}
			NumberCondition condition = searchSpec.getCondition();
			double comparisonValue = searchSpec.getSearchValue();
			double equalityTolerance = searchSpec.getEqualityTolerance();
			if (condition.equals(NumberCondition.EQUALS)) {
				return documentsTable.getDocumentsWhereValueEquals(indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue, equalityTolerance);
			} else if (condition.equals(NumberCondition.NOT_EQUALS)) {
				return documentsTable.getDocumentsWhereValueNotEquals(indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue, equalityTolerance);
			} else if (condition.equals(NumberCondition.GREATER_THAN)) {
				return documentsTable.getDocumentsWhereValueIsGreaterThan(indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue);
			} else if (condition.equals(NumberCondition.GREATER_EQUAL)) {
				return documentsTable.getDocumentsWhereValueIsGreaterOrEqual(indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue);
			} else if (condition.equals(NumberCondition.LESS_THAN)) {
				return documentsTable.getDocumentsWhereValueIsLessThan(indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue);
			} else if (condition.equals(NumberCondition.LESS_EQUAL)) {
				return documentsTable.getDocumentsWhereValueIsLessOrEqual(indexName, branchName, keyspace, timestamp, timeSearchMode, comparisonValue);
			} else {
				throw new IllegalStateException("Unknown StringCondition: '" + condition.getClass().getName() + "'!");
			}
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Could not query Index Documents Table!", e);
		}
	}

	private Set<ChronoIndexDocument> getMatchingDocumentsContains(final JdbcStringIndexDocumentTable documentsTable, final String indexName, final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final String comparisonValue, final TextMatchMode matchMode) {
		String realComparisonValue = "%" + this.escapeSQL(this.normalize(comparisonValue, matchMode), '|') + "%";
		return documentsTable.getDocumentsWhereLike(indexName, branchName, keyspace, timestamp, timeSearchMode, realComparisonValue, '|', matchMode);
	}

	private Set<ChronoIndexDocument> getMatchingDocumentsNotContains(final JdbcStringIndexDocumentTable documentsTable, final String indexName, final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final String comparisonValue, final TextMatchMode matchMode) {
		String realComparisonValue = "%" + this.escapeSQL(this.normalize(comparisonValue, matchMode), '|') + "%";
		return documentsTable.getDocumentsWhereNotLike(indexName, branchName, keyspace, timestamp, timeSearchMode, realComparisonValue, '|', matchMode);
	}

	private Set<ChronoIndexDocument> getMatchingDocumentsEndsWith(final JdbcStringIndexDocumentTable documentsTable, final String indexName, final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final String comparisonValue, final TextMatchMode matchMode) {
		String realComparisonValue = "%" + this.escapeSQL(this.normalize(comparisonValue, matchMode), '|');
		return documentsTable.getDocumentsWhereLike(indexName, branchName, keyspace, timestamp, timeSearchMode, realComparisonValue, '|', matchMode);
	}

	private Set<ChronoIndexDocument> getMatchingDocumentsNotEndsWith(final JdbcStringIndexDocumentTable documentsTable, final String indexName, final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final String comparisonValue, final TextMatchMode matchMode) {
		String realComparisonValue = "%" + this.escapeSQL(this.normalize(comparisonValue, matchMode), '|');
		return documentsTable.getDocumentsWhereNotLike(indexName, branchName, keyspace, timestamp, timeSearchMode, realComparisonValue, '|', matchMode);
	}

	private Set<ChronoIndexDocument> getMatchingDocumentsStartsWith(final JdbcStringIndexDocumentTable documentsTable, final String indexName, final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final String comparisonValue, final TextMatchMode matchMode) {
		String realComparisonValue = this.escapeSQL(this.normalize(comparisonValue, matchMode), '|') + "%";
		return documentsTable.getDocumentsWhereLike(indexName, branchName, keyspace, timestamp, timeSearchMode, realComparisonValue, '|', matchMode);
	}

	private Set<ChronoIndexDocument> getMatchingDocumentsNotStartsWith(final JdbcStringIndexDocumentTable documentsTable, final String indexName, final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final String comparisonValue, final TextMatchMode matchMode) {
		String realComparisonValue = this.escapeSQL(this.normalize(comparisonValue, matchMode), '|') + "%";
		return documentsTable.getDocumentsWhereNotLike(indexName, branchName, keyspace, timestamp, timeSearchMode, realComparisonValue, '|', matchMode);
	}

	private Set<ChronoIndexDocument> getMatchingDocumentsEquals(final JdbcStringIndexDocumentTable documentsTable, final String indexName, final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final String comparisonValue, final TextMatchMode matchMode) {
		String realComparisonValue = this.escapeSQL(this.normalize(comparisonValue, matchMode), '|');
		return documentsTable.getDocumentsWhereLike(indexName, branchName, keyspace, timestamp, timeSearchMode, realComparisonValue, '|', matchMode);
	}

	private Set<ChronoIndexDocument> getMatchingDocumentsNotEquals(final JdbcStringIndexDocumentTable documentsTable, final String indexName, final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final String comparisonValue, final TextMatchMode matchMode) {
		String realComparisonValue = this.escapeSQL(this.normalize(comparisonValue, matchMode), '|');
		return documentsTable.getDocumentsWhereNotLike(indexName, branchName, keyspace, timestamp, timeSearchMode, realComparisonValue, '|', matchMode);
	}

	private Set<ChronoIndexDocument> getMatchingDocumentsMatchesRegex(final JdbcStringIndexDocumentTable documentsTable, final String indexName, final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final String comparisonValue, final TextMatchMode matchMode) {
		// this one is different because there is no database-independent way to execute a java.util.regex in a query.
		// For that reason, we apply the following strategy:
		// 1) Retrieve all indexed values that occur (at our timestamp)
		// 2) Filter the indexed values by applying the regex at the client
		// 3) For each remaining indexed value, retrieve the documents by running one query for each of them
		// This procedure is VERY inefficient, but unfortunately the only viable way to support this feature.
		Set<String> indexedValues = documentsTable.getIndexedValues(indexName, branchName, keyspace, timestamp, timeSearchMode);
		// in case of the MATCHES operator, the comparison value contains the REGEX
		String expression;
		switch (matchMode) {
		case STRICT:
			expression = comparisonValue;
			break;
		case CASE_INSENSITIVE:
			expression = comparisonValue;
			if (comparisonValue.startsWith("(?i)") == false) {
				expression = "(?i)" + comparisonValue;
			}
			break;
		default:
			throw new UnknownEnumLiteralException(matchMode);
		}
		final String regex = expression;
		Set<String> matchingValues = indexedValues.parallelStream().filter(value -> value.matches(regex)).collect(Collectors.toSet());
		// for each value, run a query to the database to retrieve the documents
		Set<ChronoIndexDocument> documents = Sets.newHashSet();
		for (String indexedValue : matchingValues) {
			// escape the value
			String escapedValue = this.escapeSQL(indexedValue, '|');
			Set<ChronoIndexDocument> resultingDocuments = documentsTable.getDocumentsWhereLike(indexName, branchName, keyspace, timestamp, timeSearchMode, escapedValue, '|', TextMatchMode.STRICT);
			documents.addAll(resultingDocuments);
		}
		return documents;
	}

	private Set<ChronoIndexDocument> getMatchingDocumentsNotMatchesRegex(final JdbcStringIndexDocumentTable documentsTable, final String indexName, final String branchName, final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final String comparisonValue, final TextMatchMode matchMode) {
		// this one is different because there is no database-independent way to execute a java.util.regex in a query.
		// For that reason, we apply the following strategy:
		// 1) Retrieve all indexed values that occur (at our timestamp)
		// 2) Filter the indexed values by applying the regex at the client and keep the ones that do NOT match
		// 3) For each remaining indexed value, retrieve the documents by running one query for each of them
		// This procedure is VERY inefficient, but unfortunately the only viable way to support this feature.
		Set<String> indexedValues = documentsTable.getIndexedValues(indexName, branchName, keyspace, timestamp, timeSearchMode);
		// in case of the MATCHES operator, the comparison value contains the REGEX
		String expression;
		switch (matchMode) {
		case STRICT:
			expression = comparisonValue;
			break;
		case CASE_INSENSITIVE:
			expression = comparisonValue;
			if (comparisonValue.startsWith("(?i)") == false) {
				expression = "(?i)" + comparisonValue;
			}
			break;
		default:
			throw new UnknownEnumLiteralException(matchMode);
		}
		final String regex = expression;
		Set<String> matchingValues = indexedValues.parallelStream().filter(value -> value.matches(regex) == false).collect(Collectors.toSet());
		// for each value, run a query to the database to retrieve the documents
		Set<ChronoIndexDocument> documents = Sets.newHashSet();
		for (String indexedValue : matchingValues) {
			// escape the value
			String escapedValue = this.escapeSQL(indexedValue, '|');
			Set<ChronoIndexDocument> resultingDocuments = documentsTable.getDocumentsWhereLike(indexName, branchName, keyspace, timestamp, timeSearchMode, escapedValue, '|', TextMatchMode.STRICT);
			documents.addAll(resultingDocuments);
		}
		return documents;
	}

	// =================================================================================================================
	// INTERNAL UTILITY METHODS
	// =================================================================================================================

	private void ensureJdbcTablesExist() {
		try (Connection connection = this.openConnection()) {
			JdbcIndexerTable.get(connection).ensureExists();
			JdbcIndexDirtyFlagsTable.get(connection).ensureExists();
			JdbcStringIndexDocumentTable.get(connection).ensureExists();
			JdbcLongIndexDocumentTable.get(connection).ensureExists();
			JdbcDoubleIndexDocumentTable.get(connection).ensureExists();
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Could not open Connection to database!", e);
		}
	}

	private Connection openConnection() throws SQLException {
		return this.getOwningDB().getDataSource().getConnection();
	}

	private String escapeSQL(final String string, final char escapeCharacter) {
		checkNotNull(string, "Precondition violation - argument 'string' must not be NULL!");
		checkNotNull(escapeCharacter, "Precondition violation - argument 'escapeCharacter' must not be NULL!");
		String result = string;
		// double up on the escape character
		result = string.replace("" + escapeCharacter, "" + escapeCharacter + escapeCharacter);
		// escape the percent symbol
		result = string.replace("%", escapeCharacter + "%");
		return result;
	}

	private String normalize(final String string, final TextMatchMode matchMode) {
		switch (matchMode) {
		case STRICT:
			return string;
		case CASE_INSENSITIVE:
			return string.toLowerCase();
		default:
			throw new UnknownEnumLiteralException(matchMode);
		}
	}

	private void persistDocument(final Connection connection, final ChronoIndexDocument document) {
		checkNotNull(connection, "Precondition violation - argument 'connection' must not be NULL!");
		checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
		String index = document.getIndexName();
		String branch = document.getBranch();
		String keyspace = document.getKeyspace();
		String key = document.getKey();
		long validFrom = document.getValidFromTimestamp();
		long validTo = document.getValidToTimestamp();
		Object value = document.getIndexedValue();
		if (value instanceof String) {
			JdbcStringIndexDocumentTable documentsTable = JdbcStringIndexDocumentTable.get(connection);
			String valueCI = ((String) value).toLowerCase();
			documentsTable.insert(branch, keyspace, key, index, (String) value, valueCI, validFrom, validTo);
		} else if (ReflectionUtils.isLongCompatible(value)) {
			JdbcLongIndexDocumentTable documentsTable = JdbcLongIndexDocumentTable.get(connection);
			documentsTable.insert(branch, keyspace, key, index, ReflectionUtils.asLong(value), validFrom, validTo);
		} else if (ReflectionUtils.isDoubleCompatible(value)) {
			JdbcDoubleIndexDocumentTable documentsTable = JdbcDoubleIndexDocumentTable.get(connection);
			documentsTable.insert(branch, keyspace, key, index, ReflectionUtils.asDouble(value), validFrom, validTo);
		} else {
			throw new IllegalStateException("Unknown index value type: '" + value.getClass().getName() + "'!");
		}
	}

	private void terminateDocumentValidity(final Connection connection, final ChronoIndexDocument indexDocument, final long timestamp) {
		checkNotNull(connection, "Precondition violation - argument 'connection' must not be NULL!");
		checkNotNull(indexDocument, "Precondition violation - argument 'indexDocument' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		Object value = indexDocument.getIndexedValue();
		if (value instanceof String) {
			JdbcStringIndexDocumentTable documentsTable = JdbcStringIndexDocumentTable.get(connection);
			documentsTable.updateValidTo(indexDocument.getDocumentId(), timestamp);
		} else if (ReflectionUtils.isLongCompatible(value)) {
			JdbcLongIndexDocumentTable documentsTable = JdbcLongIndexDocumentTable.get(connection);
			documentsTable.updateValidTo(indexDocument.getDocumentId(), timestamp);
		} else if (ReflectionUtils.isDoubleCompatible(value)) {
			JdbcDoubleIndexDocumentTable documentsTable = JdbcDoubleIndexDocumentTable.get(connection);
			documentsTable.updateValidTo(indexDocument.getDocumentId(), timestamp);
		}
		// database operation successful, update the bean
		indexDocument.setValidToTimestamp(timestamp);
	}

	private void deleteIndexDocument(final Connection connection, final ChronoIndexDocument documentToDelete) {
		checkNotNull(connection, "Precondition violation - argument 'connection' must not be NULL!");
		checkNotNull(documentToDelete, "Precondition violation - argument 'documentToDelete' must not be NULL!");
		JdbcStringIndexDocumentTable.get(connection).delete(documentToDelete);
		JdbcLongIndexDocumentTable.get(connection).delete(documentToDelete);
		JdbcDoubleIndexDocumentTable.get(connection).delete(documentToDelete);
	}

	private JdbcChronoDB getOwningDB() {
		return (JdbcChronoDB) this.owningDB;
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	protected enum TimeSearchMode {

		VALID_AT_TIMESTAMP, TERMINATED_AT_OR_BEFORE_TIMESTAMP;

	}

}
