package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.internal.impl.jdbc.table.DefaultJdbcTable;
import org.chronos.chronodb.internal.impl.jdbc.table.IndexDeclaration;
import org.chronos.chronodb.internal.impl.jdbc.table.TableColumn;
import org.chronos.chronodb.internal.impl.jdbc.util.NamedParameterStatement;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import com.google.common.collect.Lists;

/**
 * The time table is intended to store the commit metadata for all branches.
 *
 * <p>
 * The time table has the following schema:
 *
 * <pre>
 * +-------------+--------------+--------------+------------+----------------+
 * |             | ID           | Branch       | commitTime | commitMetadata |
 * +-------------+--------------+--------------+------------+----------------+
 * | TYPEBOUND   | VARCHAR(255) | VARCHAR(255) | BIGINT     | VARBINARY(MAX) |
 * | CONSTRAINTS | PRIMARY KEY  | NOT NULL     | NOT NULL   | NOT NULL       |
 * +-------------+--------------+--------------+------------+----------------+
 * </pre>
 *
 * There is exactly one instance of this table per {@link ChronoDB}. The name of this table is stored in the constant
 * {@link JdbcCommitMetadataTable#NAME}.
 *
 * <p>
 * This class has <tt>default</tt> visibility (<tt>friendly</tt> visibility) on purpose. It is not intended to be used
 * outside of the package it resides in.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
/* default */ class JdbcCommitMetadataTable extends DefaultJdbcTable {

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	/**
	 * Grants access to the commit metadata table, using the given connection.
	 *
	 * @param connection
	 *            The connection to use for communication. Must not be <code>null</code>. Must be open.
	 * @return The commit metadata table. Never <code>null</code>.
	 */
	public static JdbcCommitMetadataTable get(final Connection connection) {
		return new JdbcCommitMetadataTable(connection);
	}

	// =================================================================================================================
	// TABLE DEFINITION
	// =================================================================================================================

	public static final String NAME = "CommitMetadataTable";

	public static final String PROPERTY_ID = "ID";
	public static final String TYPEBOUND_ID = "VARCHAR(255) NOT NULL";

	public static final String PROPERTY_BRANCH = "branch";
	public static final String TYPEBOUND_BRANCH = "VARCHAR(255) NOT NULL";

	public static final String PROPERTY_TIMESTAMP = "commitTime";
	public static final String TYPEBOUND_TIMESTAMP = "BIGINT NOT NULL";

	public static final String PROPERTY_METADATA = "commitMetadata";
	public static final String TYPEBOUND_METADATA = "VARBINARY(MAX)";

	public static TableColumn[] COLUMNS = {
			//
			new TableColumn(PROPERTY_ID, TYPEBOUND_ID),
			//
			new TableColumn(PROPERTY_BRANCH, TYPEBOUND_BRANCH),
			//
			new TableColumn(PROPERTY_TIMESTAMP, TYPEBOUND_TIMESTAMP),
			//
			new TableColumn(PROPERTY_METADATA, TYPEBOUND_METADATA)
			//
	};

	public static IndexDeclaration[] INDICES = {
			//
			new IndexDeclaration(NAME + "_BranchIndex", PROPERTY_BRANCH),
			//
			new IndexDeclaration(NAME + "_TimeIndex", PROPERTY_TIMESTAMP)
			//
	};

	// =====================================================================================================================
	// SQL STATEMENTS
	// =====================================================================================================================

	public static final String NAMED_SQL__INSERT = "INSERT INTO " + NAME
			+ " VALUES(${id}, ${branch}, ${timestamp}, ${metadata})";

	public static final String NAMED_SQL__GET_METADATA_FOR_BRANCH_AND_TIMESTAMP = "SELECT " + PROPERTY_METADATA
			+ " FROM " + NAME + " WHERE " + PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_TIMESTAMP
			+ " = ${timestamp}";

	public static final String NAMED_SQL__ROLLBACK_BRANCH_TO_TIMESTAMP = "DELETE FROM " + NAME + " WHERE "
			+ PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_TIMESTAMP + " > ${timestamp}";

	public static final String NAMED_SQL__GET_COMMIT_TIMESTAMPS_BETWEEN_ASC = "SELECT DISTINCT " + PROPERTY_TIMESTAMP
			+ " FROM " + NAME + " WHERE " + PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_TIMESTAMP
			+ " >= ${from} AND " + PROPERTY_TIMESTAMP + " <= ${to} ORDER BY " + PROPERTY_TIMESTAMP + " ASC";

	public static final String NAMED_SQL__GET_COMMIT_TIMESTAMPS_BETWEEN_DESC = "SELECT DISTINCT " + PROPERTY_TIMESTAMP
			+ " FROM " + NAME + " WHERE " + PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_TIMESTAMP
			+ " >= ${from} AND " + PROPERTY_TIMESTAMP + " <= ${to} ORDER BY " + PROPERTY_TIMESTAMP + " DESC";

	// TODO Limit/Offset is MySQL syntax; other databases, e.g. SQL Server, won't recognize this query!
	public static final String NAMED_SQL__GET_COMMIT_TIMESTAMPS_PAGED_ASC = "SELECT DISTINCT " + PROPERTY_TIMESTAMP
			+ " FROM " + NAME + " WHERE " + PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_TIMESTAMP
			+ " >= ${from} AND " + PROPERTY_TIMESTAMP + " <= ${to} ORDER BY " + PROPERTY_TIMESTAMP
			+ " ASC LIMIT ${limit} OFFSET ${offset}";

	// TODO Limit/Offset is MySQL syntax; other databases, e.g. SQL Server, won't recognize this query!
	public static final String NAMED_SQL__GET_COMMIT_TIMESTAMPS_PAGED_DESC = "SELECT DISTINCT " + PROPERTY_TIMESTAMP
			+ " FROM " + NAME + " WHERE " + PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_TIMESTAMP
			+ " >= ${from} AND " + PROPERTY_TIMESTAMP + " <= ${to} ORDER BY " + PROPERTY_TIMESTAMP
			+ " DESC LIMIT ${limit} OFFSET ${offset}";

	public static final String NAMED_SQL__GET_COMMIT_METADATA_BETWEEN_ASC = "SELECT DISTINCT " + PROPERTY_TIMESTAMP
			+ ", " + PROPERTY_METADATA + " FROM " + NAME + " WHERE " + PROPERTY_BRANCH + " = ${branch} AND "
			+ PROPERTY_TIMESTAMP + " >= ${from} AND " + PROPERTY_TIMESTAMP + " <= ${to} ORDER BY " + PROPERTY_TIMESTAMP
			+ " ASC";

	public static final String NAMED_SQL__GET_COMMIT_METADATA_BETWEEN_DESC = "SELECT DISTINCT " + PROPERTY_TIMESTAMP
			+ ", " + PROPERTY_METADATA + " FROM " + NAME + " WHERE " + PROPERTY_BRANCH + " = ${branch} AND "
			+ PROPERTY_TIMESTAMP + " >= ${from} AND " + PROPERTY_TIMESTAMP + " <= ${to} ORDER BY " + PROPERTY_TIMESTAMP
			+ " DESC";

	// TODO Limit/Offset is MySQL syntax; other databases, e.g. SQL Server, won't recognize this query!
	public static final String NAMED_SQL__GET_COMMIT_METADATA_PAGED_ASC = "SELECT DISTINCT " + PROPERTY_TIMESTAMP + ", "
			+ PROPERTY_METADATA + " FROM " + NAME + " WHERE " + PROPERTY_BRANCH + " = ${branch} AND "
			+ PROPERTY_TIMESTAMP + " >= ${from} AND " + PROPERTY_TIMESTAMP + " <= ${to} ORDER BY " + PROPERTY_TIMESTAMP
			+ " ASC LIMIT ${limit} OFFSET ${offset}";

	// TODO Limit/Offset is MySQL syntax; other databases, e.g. SQL Server, won't recognize this query!
	public static final String NAMED_SQL__GET_COMMIT_METADATA_PAGED_DESC = "SELECT DISTINCT " + PROPERTY_TIMESTAMP
			+ ", " + PROPERTY_METADATA + " FROM " + NAME + " WHERE " + PROPERTY_BRANCH + " = ${branch} AND "
			+ PROPERTY_TIMESTAMP + " >= ${from} AND " + PROPERTY_TIMESTAMP + " <= ${to} ORDER BY " + PROPERTY_TIMESTAMP
			+ " DESC LIMIT ${limit} OFFSET ${offset}";

	public static final String NAMED_SQL__COUNT_COMMIT_TIMESTAMPS_BETWEEN = "SELECT COUNT(*) FROM " + NAME + " WHERE "
			+ PROPERTY_BRANCH + " = ${branch} AND " + PROPERTY_TIMESTAMP + " >= ${from} AND " + PROPERTY_TIMESTAMP
			+ " <= ${to}";

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected JdbcCommitMetadataTable(final Connection connection) {
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

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public void insert(final String branchName, final long timestamp, final byte[] metadata) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		String id = UUID.randomUUID().toString();
		String sql = NAMED_SQL__INSERT;
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("id", id);
			nStmt.setParameter("branch", branchName);
			nStmt.setParameter("timestamp", timestamp);
			nStmt.setParameter("metadata", metadata);
			nStmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to insert rows into Commit Metadata Table!", e);
		}
	}

	public byte[] getCommitMetadata(final String branchName, final long timestamp) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		String sql = NAMED_SQL__GET_METADATA_FOR_BRANCH_AND_TIMESTAMP;
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("branch", branchName);
			nStmt.setParameter("timestamp", timestamp);
			try (ResultSet resultSet = nStmt.executeQuery()) {
				if (resultSet.next() == false) {
					// we have no commit metadata for this branch and timestamp
					return null;
				}
				byte[] result = null;
				Blob blob = resultSet.getBlob(PROPERTY_METADATA);
				byte[] bytes = null;
				try {
					bytes = blob.getBytes(1, (int) blob.length());
				} finally {
					blob.free();
				}
				result = bytes;
				if (resultSet.next()) {
					throw new IllegalStateException("Found multiple commit metadata objects for branch '" + branchName
							+ "' and timestamp " + timestamp + "!");
				}
				return result;
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to read from Commit Metadata Table!", e);
		}
	}

	public void rollbackBranchToTimestamp(final String branchName, final long timestamp) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		String sql = NAMED_SQL__ROLLBACK_BRANCH_TO_TIMESTAMP;
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("branch", branchName);
			nStmt.setParameter("timestamp", timestamp);
			nStmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to roll back Commit Metadata Table!", e);
		}
	}

	public Iterator<Long> getCommitTimestampsBetween(final String branchName, final long from, final long to,
			final Order order) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		final String sql;
		switch (order) {
		case ASCENDING:
			sql = NAMED_SQL__GET_COMMIT_TIMESTAMPS_BETWEEN_ASC;
			break;
		case DESCENDING:
			sql = NAMED_SQL__GET_COMMIT_TIMESTAMPS_BETWEEN_DESC;
			break;
		default:
			throw new UnknownEnumLiteralException(order);
		}
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("branch", branchName);
			nStmt.setParameter("from", from);
			nStmt.setParameter("to", to);
			nStmt.setParameter("order", orderToSQL(order));
			try (ResultSet resultSet = nStmt.executeQuery()) {
				List<Long> list = Lists.newArrayList();
				while (resultSet.next()) {
					list.add(resultSet.getLong(PROPERTY_TIMESTAMP));
				}
				return Collections.unmodifiableList(list).iterator();
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to read from Commit Metadata Table!", e);
		}
	}

	public Iterator<Entry<Long, byte[]>> getCommitMetadataBetween(final String branchName, final long from,
			final long to, final Order order) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		final String sql;
		switch (order) {
		case ASCENDING:
			sql = NAMED_SQL__GET_COMMIT_METADATA_BETWEEN_ASC;
			break;
		case DESCENDING:
			sql = NAMED_SQL__GET_COMMIT_METADATA_BETWEEN_DESC;
			break;
		default:
			throw new UnknownEnumLiteralException(order);
		}
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("branch", branchName);
			nStmt.setParameter("from", from);
			nStmt.setParameter("to", to);
			nStmt.setParameter("order", orderToSQL(order));
			try (ResultSet resultSet = nStmt.executeQuery()) {
				List<Entry<Long, byte[]>> list = Lists.newArrayList();
				while (resultSet.next()) {
					long timestamp = resultSet.getLong(PROPERTY_TIMESTAMP);
					Blob blob = resultSet.getBlob(PROPERTY_METADATA);
					byte[] bytes = null;
					try {
						bytes = blob.getBytes(1, (int) blob.length());
					} finally {
						blob.free();
					}
					list.add(Pair.of(timestamp, bytes));
				}
				return Collections.unmodifiableList(list).iterator();
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to read from Commit Metadata Table!", e);
		}
	}

	public Iterator<Long> getCommitTimestampsPaged(final String branchName, final long minTimestamp,
			final long maxTimestamp, final int pageSize, final int pageIndex, final Order order) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
		checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
		checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
		checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		final String sql;
		switch (order) {
		case ASCENDING:
			sql = NAMED_SQL__GET_COMMIT_TIMESTAMPS_PAGED_ASC;
			break;
		case DESCENDING:
			sql = NAMED_SQL__GET_COMMIT_TIMESTAMPS_PAGED_DESC;
			break;
		default:
			throw new UnknownEnumLiteralException(order);
		}
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("branch", branchName);
			nStmt.setParameter("from", minTimestamp);
			nStmt.setParameter("to", maxTimestamp);
			nStmt.setParameter("order", orderToSQL(order));
			nStmt.setParameter("limit", pageSize);
			nStmt.setParameter("offset", pageIndex * pageSize);
			try (ResultSet resultSet = nStmt.executeQuery()) {
				List<Long> list = Lists.newArrayList();
				while (resultSet.next()) {
					list.add(resultSet.getLong(PROPERTY_TIMESTAMP));
				}
				return Collections.unmodifiableList(list).iterator();
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to read from Commit Metadata Table!", e);
		}
	}

	public Iterator<Entry<Long, byte[]>> getCommitMetadataPaged(final String branchName, final long minTimestamp,
			final long maxTimestamp, final int pageSize, final int pageIndex, final Order order) {
		checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
		checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
		checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
		checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		final String sql;
		switch (order) {
		case ASCENDING:
			sql = NAMED_SQL__GET_COMMIT_METADATA_PAGED_ASC;
			break;
		case DESCENDING:
			sql = NAMED_SQL__GET_COMMIT_METADATA_PAGED_DESC;
			break;
		default:
			throw new UnknownEnumLiteralException(order);
		}
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("branch", branchName);
			nStmt.setParameter("from", minTimestamp);
			nStmt.setParameter("to", maxTimestamp);
			nStmt.setParameter("order", orderToSQL(order));
			nStmt.setParameter("limit", pageSize);
			nStmt.setParameter("offset", pageIndex * pageSize);
			try (ResultSet resultSet = nStmt.executeQuery()) {
				List<Entry<Long, byte[]>> list = Lists.newArrayList();
				while (resultSet.next()) {
					long timestamp = resultSet.getLong(PROPERTY_TIMESTAMP);
					Blob blob = resultSet.getBlob(PROPERTY_METADATA);
					byte[] bytes = null;
					try {
						bytes = blob.getBytes(1, (int) blob.length());
					} finally {
						blob.free();
					}
					list.add(Pair.of(timestamp, bytes));
				}
				return Collections.unmodifiableList(list).iterator();
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to read from Commit Metadata Table!", e);
		}
	}

	public int countCommitTimestampsBetween(final String branchName, final long from, final long to) {
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		String sql = NAMED_SQL__COUNT_COMMIT_TIMESTAMPS_BETWEEN;
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("branch", branchName);
			nStmt.setParameter("from", from);
			nStmt.setParameter("to", to);
			try (ResultSet resultSet = nStmt.executeQuery()) {
				if (resultSet.next() == false) {
					return 0;
				} else {
					return resultSet.getInt(1);
				}
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to read from Commit Metadata Table!", e);
		}
	}

	// =====================================================================================================================
	// UTILITY METHODS
	// =====================================================================================================================

	private static String orderToSQL(final Order order) {
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		switch (order) {
		case ASCENDING:
			return "ASC";
		case DESCENDING:
			return "DESC";
		default:
			throw new UnknownEnumLiteralException(order);
		}
	}

}
