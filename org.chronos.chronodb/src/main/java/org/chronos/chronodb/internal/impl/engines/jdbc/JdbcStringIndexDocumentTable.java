package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.impl.engines.jdbc.JdbcIndexManagerBackend.TimeSearchMode;
import org.chronos.chronodb.internal.impl.index.ChronoIndexDocumentImpl;
import org.chronos.chronodb.internal.impl.jdbc.table.DefaultJdbcTable;
import org.chronos.chronodb.internal.impl.jdbc.table.IndexDeclaration;
import org.chronos.chronodb.internal.impl.jdbc.table.TableColumn;
import org.chronos.chronodb.internal.impl.jdbc.util.JdbcUtils;
import org.chronos.chronodb.internal.impl.jdbc.util.NamedParameterStatement;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.logging.ChronoLogger;

import com.google.common.collect.Sets;

class JdbcStringIndexDocumentTable extends DefaultJdbcTable {

	// =================================================================================================================
	// FACTORY
	// =================================================================================================================

	public static JdbcStringIndexDocumentTable get(final Connection connection) {
		return new JdbcStringIndexDocumentTable(connection);
	}

	// =================================================================================================================
	// TABLE DEFINITION
	// =================================================================================================================

	private static final String NAME = "IndexDocuments";

	private static final String PROPERTY_ID = "id";
	private static final String TYPEBOUND_ID = "VARCHAR(255) NOT NULL";

	private static final String PROPERTY_BRANCH = "branch";
	private static final String TYPEBOUND_BRANCH = "VARCHAR(255) NOT NULL";

	private static final String PROPERTY_KEYSPACE = "keyspace";
	private static final String TYPEBOUND_KEYSPACE = "VARCHAR(255) NOT NULL";

	private static final String PROPERTY_KEY = "mapkey";
	private static final String TYPEBOUND_KEY = "VARCHAR(255) NOT NULL";

	private static final String PROPERTY_INDEX_NAME = "indexname";
	private static final String TYPEBOUND_INDEX_NAME = "VARCHAR(255) NOT NULL";

	private static final String PROPERTY_INDEXED_VALUE = "indexedvalue";
	private static final String TYPEBOUND_INDEXED_VALUE = "VARCHAR(255) NOT NULL";

	private static final String PROPERTY_INDEXED_VALUE_CI = "indexedvalue_ci";
	private static final String TYPEBOUND_INDEXED_VALUE_CI = "VARCHAR(255) NOT NULL";

	private static final String PROPERTY_VALID_FROM = "validfrom";
	private static final String TYPEBOUND_VALID_FROM = "BIGINT NOT NULL";

	private static final String PROPERTY_VALID_TO = "validto";
	private static final String TYPEBOUND_VALID_TO = "BIGINT NOT NULL";

	private static final TableColumn[] COLUMNS = {
			//
			new TableColumn(PROPERTY_ID, TYPEBOUND_ID),
			//
			new TableColumn(PROPERTY_BRANCH, TYPEBOUND_BRANCH),
			//
			new TableColumn(PROPERTY_KEYSPACE, TYPEBOUND_KEYSPACE),
			//
			new TableColumn(PROPERTY_KEY, TYPEBOUND_KEY),
			//
			new TableColumn(PROPERTY_INDEX_NAME, TYPEBOUND_INDEX_NAME),
			//
			new TableColumn(PROPERTY_INDEXED_VALUE, TYPEBOUND_INDEXED_VALUE),
			//
			new TableColumn(PROPERTY_INDEXED_VALUE_CI, TYPEBOUND_INDEXED_VALUE_CI),
			//
			new TableColumn(PROPERTY_VALID_FROM, TYPEBOUND_VALID_FROM),
			//
			new TableColumn(PROPERTY_VALID_TO, TYPEBOUND_VALID_TO)
			//
	};

	private static final IndexDeclaration[] INDICES = {
			//
			new IndexDeclaration(NAME + "_BranchIndex", PROPERTY_BRANCH),
			//
			new IndexDeclaration(NAME + "_KeyspaceIndex", PROPERTY_KEYSPACE),
			//
			new IndexDeclaration(NAME + "_KeyIndex", PROPERTY_KEY),
			//
			new IndexDeclaration(NAME + "_IndexName", PROPERTY_INDEX_NAME),
			//
			new IndexDeclaration(NAME + "_IndexedValue", PROPERTY_INDEXED_VALUE),
			//
			new IndexDeclaration(NAME + "_IndexedValueCI", PROPERTY_INDEXED_VALUE_CI),
			//
			new IndexDeclaration(NAME + "_ValidFrom", PROPERTY_VALID_FROM),
			//
			new IndexDeclaration(NAME + "_ValidTo", PROPERTY_VALID_TO)

	};

	// =================================================================================================================
	// SQL STATEMENTS
	// =================================================================================================================

	private static final String SQL_INSERT = "INSERT INTO " + NAME + " VALUES(?,?,?,?,?,?,?,?,?)";

	private static final String SQL_UPDATE_VALID_TO = "UPDATE " + NAME + " SET " + PROPERTY_VALID_TO + " = ? "
			+ " WHERE " + PROPERTY_ID + " = ?";

	private static final String NAMED_SQL_GET_DOCUMENTS_WHERE_LIKE_STRICT_VALID_AT = "SELECT * FROM " + NAME + " WHERE "
			+ PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_KEYSPACE + " = ${keyspace} AND " + PROPERTY_INDEX_NAME + " = ${index} AND " + PROPERTY_VALID_FROM
			+ " <= ${timestamp} AND " + PROPERTY_VALID_TO + " > ${timestamp} AND " + PROPERTY_INDEXED_VALUE
			+ " LIKE ${search} ESCAPE ${escape}";

	private static final String NAMED_SQL_GET_DOCUMENTS_WHERE_LIKE_STRICT_TERMINATED_UNTIL = "SELECT * FROM " + NAME
			+ " WHERE " + PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_KEYSPACE + " = ${keyspace} AND " + PROPERTY_INDEX_NAME + " = ${index} AND "
			+ PROPERTY_VALID_TO + " <= ${timestamp} AND " + PROPERTY_INDEXED_VALUE + " LIKE ${search} ESCAPE ${escape}";

	private static final String NAMED_SQL_GET_DOCUMENTS_WHERE_LIKE_CI_VALID_AT = "SELECT * FROM " + NAME + " WHERE "
			+ PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_KEYSPACE + " = ${keyspace} AND " + PROPERTY_INDEX_NAME + " = ${index} AND " + PROPERTY_VALID_FROM
			+ " <= ${timestamp} AND " + PROPERTY_VALID_TO + " > ${timestamp} AND " + PROPERTY_INDEXED_VALUE_CI
			+ " LIKE ${search} ESCAPE ${escape}";

	private static final String NAMED_SQL_GET_DOCUMENTS_WHERE_LIKE_CI_TERMINATED_UNTIL = "SELECT * FROM " + NAME
			+ " WHERE " + PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_KEYSPACE + " = ${keyspace} AND " + PROPERTY_INDEX_NAME + " = ${index} AND "
			+ PROPERTY_VALID_TO + " <= ${timestamp} AND " + PROPERTY_INDEXED_VALUE_CI
			+ " LIKE ${search} ESCAPE ${escape}";

	private static final String NAMED_SQL_GET_DOCUMENTS_WHERE_NOT_LIKE_STRICT_VALID_AT = "SELECT * FROM " + NAME
			+ " WHERE " + PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_KEYSPACE + " = ${keyspace} AND " + PROPERTY_INDEX_NAME + " = ${index} AND "
			+ PROPERTY_VALID_FROM + " <= ${timestamp} AND " + PROPERTY_VALID_TO + " > ${timestamp} AND "
			+ PROPERTY_INDEXED_VALUE + " NOT LIKE ${search} ESCAPE ${escape}";

	private static final String NAMED_SQL_GET_DOCUMENTS_WHERE_NOT_LIKE_STRICT_TERMINATED_UNTIL = "SELECT * FROM " + NAME
			+ " WHERE " + PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_KEYSPACE + " = ${keyspace} AND " + PROPERTY_INDEX_NAME + " = ${index} AND "
			+ PROPERTY_VALID_TO + " <= ${timestamp} AND " + PROPERTY_INDEXED_VALUE
			+ " NOT LIKE ${search} ESCAPE ${escape}";

	private static final String NAMED_SQL_GET_DOCUMENTS_WHERE_NOT_LIKE_CI_VALID_AT = "SELECT * FROM " + NAME + " WHERE "
			+ PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_KEYSPACE + " = ${keyspace} AND " + PROPERTY_INDEX_NAME + " = ${index} AND " + PROPERTY_VALID_FROM
			+ " <= ${timestamp} AND " + PROPERTY_VALID_TO + " > ${timestamp} AND " + PROPERTY_INDEXED_VALUE_CI
			+ " NOT LIKE ${search} ESCAPE ${escape}";

	private static final String NAMED_SQL_GET_DOCUMENTS_WHERE_NOT_LIKE_CI_TERMINATED_UNTIL = "SELECT * FROM " + NAME
			+ " WHERE " + PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_KEYSPACE + " = ${keyspace} AND " + PROPERTY_INDEX_NAME + " = ${index} AND "
			+ PROPERTY_VALID_TO + " <= ${timestamp} AND " + PROPERTY_INDEXED_VALUE_CI
			+ " NOT LIKE ${search} ESCAPE ${escape}";

	private static final String NAMED_SQL_GET_INDEXED_VALUES_VALID_AT_TIMESTAMP = "SELECT DISTINCT "
			+ PROPERTY_INDEXED_VALUE + " FROM " + NAME + " WHERE " + PROPERTY_BRANCH + " = ${branch} AND "
			+ PROPERTY_KEYSPACE + " = ${keyspace} AND "
			+ PROPERTY_INDEX_NAME + " = ${index} AND " + PROPERTY_VALID_FROM + " <= ${timestamp} AND "
			+ PROPERTY_VALID_TO + " > ${timestamp}";

	private static final String NAMED_SQL_GET_INDEXED_VALUES_TERMINATED_UP_UNTIL = "SELECT DISTINCT "
			+ PROPERTY_INDEXED_VALUE + " FROM " + NAME + " WHERE " + PROPERTY_BRANCH + " = ${branch} AND "
			+ PROPERTY_KEYSPACE + " = ${keyspace} AND "
			+ PROPERTY_INDEX_NAME + " = ${index} AND " + PROPERTY_VALID_TO + " <= ${timestamp}";

	private static final String NAMED_SQL_DELETE_WHERE_DOCUMENT_ID_EQUALS = "DELETE FROM " + NAME + " WHERE "
			+ PROPERTY_ID + " = ${documentId}";

	private static final String NAMED_SQL_GET_MATCHING_BRANCH_LOCAL_DOCUMENTS_FOR_IDENTIFIER = "SELECT * FROM " + NAME
			+ " WHERE " + PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_KEYSPACE + " = ${keyspace} AND "
			+ PROPERTY_KEY + " = ${key} AND " + PROPERTY_VALID_FROM + " <= ${timestamp} AND " + PROPERTY_VALID_TO
			+ " > ${timestamp}";

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected JdbcStringIndexDocumentTable(final Connection connection) {
		super(connection);
	}

	// =================================================================================================================
	// API IMPLEMENTATION
	// =================================================================================================================

	@Override
	protected String getName() {
		return NAME;
	}

	@Override
	protected TableColumn[] getColumns() {
		return COLUMNS;
	}

	@Override
	protected IndexDeclaration[] getIndexDeclarations() {
		return INDICES;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	public ChronoIndexDocument insert(final String branch, final String keyspace, final String key,
			final String indexName, final String indexedValue, final String indexedValueCaseInsensitive,
			final long validFrom, final long validTo) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(indexedValue, "Precondition violation - argument 'indexedValue' must not be NULL!");
		checkNotNull(indexedValueCaseInsensitive,
				"Precondition violation - argument 'indexedValueCaseInsensitive' must not be NULL!");
		checkArgument(validFrom >= 0, "Precondition violation - argument 'validFrom' must not be negative!");
		checkArgument(validTo > 0, "Precondition violation - argument 'validTo' must not be negative!");
		checkArgument(validFrom < validTo, "Precondition violation - argument 'validFrom' must be < 'validTo'!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_INSERT)) {
			String id = UUID.randomUUID().toString();
			pstmt.setString(1, id);
			pstmt.setString(2, branch);
			pstmt.setString(3, keyspace);
			pstmt.setString(4, key);
			pstmt.setString(5, indexName);
			pstmt.setString(6, indexedValue);
			pstmt.setString(7, indexedValueCaseInsensitive);
			pstmt.setLong(8, validFrom);
			pstmt.setLong(9, validTo);
			ChronoLogger.logDebug("INSERTING INDEX DOCUMENT: " + JdbcUtils.resolvePreparedStatement(SQL_INSERT, id,
					branch, keyspace, key, indexName, indexedValue, indexedValueCaseInsensitive, validFrom, validTo));
			pstmt.executeUpdate();
			ChronoIndexDocument document = new ChronoIndexDocumentImpl(id, indexName, branch, keyspace, key,
					indexedValue, validFrom, validTo);
			return document;
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not insert into Index Documents Table!", e);
		}
	}

	public void updateValidTo(final String id, final long newValidTo) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		checkArgument(newValidTo > 0, "Precondition violation - argument 'newValidTo' must not be negative!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_UPDATE_VALID_TO)) {
			pstmt.setLong(1, newValidTo);
			pstmt.setString(2, id);
			int changedRows = pstmt.executeUpdate();
			if (changedRows <= 0) {
				throw new ChronoDBStorageBackendException(
						"Failed to set 'validTo' property of document with ID '" + id + "'!");
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not update entry in Index Documents Table!", e);
		}
	}

	public Set<ChronoIndexDocument> getDocumentsWhereLike(final String indexName, final String branch,
			final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final String likeExpression,
			final char escapeCharacter, final TextMatchMode matchMode) {
		String sql;
		switch (matchMode) {
		case STRICT:
			switch (timeSearchMode) {
			case TERMINATED_AT_OR_BEFORE_TIMESTAMP:
				sql = NAMED_SQL_GET_DOCUMENTS_WHERE_LIKE_STRICT_TERMINATED_UNTIL;
				break;
			case VALID_AT_TIMESTAMP:
				sql = NAMED_SQL_GET_DOCUMENTS_WHERE_LIKE_STRICT_VALID_AT;
				break;
			default:
				throw new UnknownEnumLiteralException(timeSearchMode);
			}
			break;
		case CASE_INSENSITIVE:
			switch (timeSearchMode) {
			case TERMINATED_AT_OR_BEFORE_TIMESTAMP:
				sql = NAMED_SQL_GET_DOCUMENTS_WHERE_LIKE_CI_TERMINATED_UNTIL;
				break;
			case VALID_AT_TIMESTAMP:
				sql = NAMED_SQL_GET_DOCUMENTS_WHERE_LIKE_CI_VALID_AT;
				break;
			default:
				throw new UnknownEnumLiteralException(timeSearchMode);
			}
			break;
		default:
			throw new UnknownEnumLiteralException(matchMode);
		}
		return this.getDocumentsWhereLikeInternal(indexName, branch, keyspace, timestamp, timeSearchMode, likeExpression,
				escapeCharacter, sql);
	}

	public Set<ChronoIndexDocument> getDocumentsWhereNotLike(final String indexName, final String branch, final String keyspace,
			final long timestamp, final TimeSearchMode timeSearchMode, final String likeExpression,
			final char escapeCharacter, final TextMatchMode matchMode) {
		String sql;
		switch (matchMode) {
		case STRICT:
			switch (timeSearchMode) {
			case TERMINATED_AT_OR_BEFORE_TIMESTAMP:
				sql = NAMED_SQL_GET_DOCUMENTS_WHERE_NOT_LIKE_STRICT_TERMINATED_UNTIL;
				break;
			case VALID_AT_TIMESTAMP:
				sql = NAMED_SQL_GET_DOCUMENTS_WHERE_NOT_LIKE_STRICT_VALID_AT;
				break;
			default:
				throw new UnknownEnumLiteralException(timeSearchMode);
			}
			break;
		case CASE_INSENSITIVE:
			switch (timeSearchMode) {
			case TERMINATED_AT_OR_BEFORE_TIMESTAMP:
				sql = NAMED_SQL_GET_DOCUMENTS_WHERE_NOT_LIKE_CI_TERMINATED_UNTIL;
				break;
			case VALID_AT_TIMESTAMP:
				sql = NAMED_SQL_GET_DOCUMENTS_WHERE_NOT_LIKE_CI_VALID_AT;
				break;
			default:
				throw new UnknownEnumLiteralException(timeSearchMode);
			}
			break;
		default:
			throw new UnknownEnumLiteralException(matchMode);
		}
		return this.getDocumentsWhereLikeInternal(indexName, branch, keyspace, timestamp, timeSearchMode, likeExpression,
				escapeCharacter, sql);
	}

	private Set<ChronoIndexDocument> getDocumentsWhereLikeInternal(final String indexName, final String branch,
			final String keyspace, final long timestamp, final TimeSearchMode timeSearchMode, final String likeExpression,
			final char escapeCharacter, final String sql) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(timeSearchMode, "Precondition violation - argument 'timeSearchMode' must not be NULL!");
		checkNotNull(likeExpression, "Precondition violation - argument 'likeExpression' must not be NULL!");
		checkNotNull(escapeCharacter, "Precondition violation - argument 'escapeCharacter' must not be NULL!");
		try (NamedParameterStatement namedStmt = new NamedParameterStatement(this.connection, sql)) {
			namedStmt.setParameter("branch", branch);
			namedStmt.setParameter("keyspace", keyspace);
			namedStmt.setParameter("index", indexName);
			namedStmt.setParameter("timestamp", timestamp);
			namedStmt.setParameter("search", likeExpression);
			namedStmt.setParameter("escape", "" + escapeCharacter);
			ChronoLogger.logTrace("SEARCH: " + namedStmt.toStringWithResolvedParameters());
			try (ResultSet resultSet = namedStmt.executeQuery()) {
				return this.convertResultSetToDocuments(resultSet);
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not query Index Documents Table!", e);
		}
	}

	public Set<String> getIndexedValues(final String indexName, final String branch, final String keyspace, final long timestamp,
			final TimeSearchMode timeSearchMode) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(timeSearchMode, "Precondition violation - argument 'timeSearchMode' must not be NULL!");
		Set<String> indexedValues = Sets.newHashSet();
		String sql = null;
		switch (timeSearchMode) {
		case TERMINATED_AT_OR_BEFORE_TIMESTAMP:
			sql = NAMED_SQL_GET_INDEXED_VALUES_TERMINATED_UP_UNTIL;
			break;
		case VALID_AT_TIMESTAMP:
			sql = NAMED_SQL_GET_INDEXED_VALUES_VALID_AT_TIMESTAMP;
			break;
		default:
			throw new UnknownEnumLiteralException(timeSearchMode);
		}
		try (NamedParameterStatement namedStmt = new NamedParameterStatement(this.connection, sql)) {
			namedStmt.setParameter("branch", branch);
			namedStmt.setParameter("keyspace", keyspace);
			namedStmt.setParameter("index", indexName);
			namedStmt.setParameter("timestamp", timestamp);
			try (ResultSet resultSet = namedStmt.executeQuery()) {
				while (resultSet.next()) {
					String anIndexedValue = resultSet.getString(PROPERTY_INDEXED_VALUE);
					indexedValues.add(anIndexedValue);
				}
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not query Index Documents Table!", e);
		}
		return indexedValues;
	}

	private String generateNamedSQLGetDocumentsTouchedAtOrAfterTimestamp(final Set<String> branches) {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * FROM ");
		sql.append(NAME);
		sql.append(" WHERE ");
		sql.append(PROPERTY_VALID_FROM);
		sql.append(" >= ${timestamp} OR (");
		sql.append(PROPERTY_VALID_TO);
		sql.append(" >= ${timestamp} AND ");
		sql.append(PROPERTY_VALID_TO);
		sql.append(" < ");
		sql.append(Long.MAX_VALUE);
		sql.append(")");
		if (branches != null) {
			sql.append(" AND ");
			sql.append(PROPERTY_BRANCH);
			sql.append(" IN (");
			String separator = "";
			for (String branch : branches) {
				sql.append(separator);
				separator = ", ";
				sql.append("'");
				sql.append(branch);
				sql.append("'");
			}
			sql.append(")");
		}
		return sql.toString();
	}

	public Set<ChronoIndexDocument> getDocumentsTouchedAtOrAfterTimestamp(final long timestamp,
			final Set<String> branches) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		String sql = this.generateNamedSQLGetDocumentsTouchedAtOrAfterTimestamp(branches);
		try (NamedParameterStatement namedStatement = new NamedParameterStatement(this.connection, sql)) {
			namedStatement.setParameter("timestamp", timestamp);
			try (ResultSet resultSet = namedStatement.executeQuery()) {
				Set<ChronoIndexDocument> documents = this.convertResultSetToDocuments(resultSet);
				return documents;
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not query Index Documents Table!", e);
		}
	}

	public Collection<ChronoIndexDocument> getMatchingBranchLocalDocuments(final ChronoIdentifier chronoIdentifier) {
		checkNotNull(chronoIdentifier, "Precondition violation - argument 'chronoIdentifier' must not be NULL!");
		String sql = NAMED_SQL_GET_MATCHING_BRANCH_LOCAL_DOCUMENTS_FOR_IDENTIFIER;
		try (NamedParameterStatement namedStatement = new NamedParameterStatement(this.connection, sql)) {
			namedStatement.setParameter("branch", chronoIdentifier.getBranchName());
			namedStatement.setParameter("keyspace", chronoIdentifier.getKeyspace());
			namedStatement.setParameter("key", chronoIdentifier.getKey());
			namedStatement.setParameter("timestamp", chronoIdentifier.getTimestamp());
			try (ResultSet resultSet = namedStatement.executeQuery()) {
				Set<ChronoIndexDocument> documents = this.convertResultSetToDocuments(resultSet);
				return documents;
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not query Index Documents Table!", e);
		}
	}

	public boolean delete(final ChronoIndexDocument documentToDelete) {
		checkNotNull(documentToDelete, "Precondition violation - argument 'documentToDelete' must not be NULL!");
		String sql = NAMED_SQL_DELETE_WHERE_DOCUMENT_ID_EQUALS;
		try (NamedParameterStatement namedStatement = new NamedParameterStatement(this.connection, sql)) {
			namedStatement.setParameter("documentId", documentToDelete.getDocumentId());
			int affectedRows = namedStatement.executeUpdate();
			return affectedRows > 0;
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not update Index Documents Table!", e);
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private Set<ChronoIndexDocument> convertResultSetToDocuments(final ResultSet resultSet) throws SQLException {
		checkNotNull(resultSet, "Precondition violation - argument 'resultSet' must not be NULL!");
		Set<ChronoIndexDocument> documents = Sets.newHashSet();
		while (resultSet.next()) {
			ChronoIndexDocument document = this.convertResultSetToDocument(resultSet);
			documents.add(document);
		}
		return documents;
	}

	private ChronoIndexDocument convertResultSetToDocument(final ResultSet resultSet) throws SQLException {
		checkNotNull(resultSet, "Precondition violation - argument 'resultSet' must not be NULL!");
		String id = resultSet.getString(PROPERTY_ID);
		String indexName = resultSet.getString(PROPERTY_INDEX_NAME);
		String branchName = resultSet.getString(PROPERTY_BRANCH);
		String keyspace = resultSet.getString(PROPERTY_KEYSPACE);
		String key = resultSet.getString(PROPERTY_KEY);
		String indexedValue = resultSet.getString(PROPERTY_INDEXED_VALUE);
		long validFrom = resultSet.getLong(PROPERTY_VALID_FROM);
		long validTo = resultSet.getLong(PROPERTY_VALID_TO);
		return new ChronoIndexDocumentImpl(id, indexName, branchName, keyspace, key, indexedValue,
				validFrom, validTo);
	}

}
