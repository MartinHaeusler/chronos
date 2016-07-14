package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.internal.impl.engines.base.KeyspaceMetadata;
import org.chronos.chronodb.internal.impl.jdbc.table.DefaultJdbcTable;
import org.chronos.chronodb.internal.impl.jdbc.table.IndexDeclaration;
import org.chronos.chronodb.internal.impl.jdbc.table.TableColumn;

import com.google.common.collect.Sets;

/**
 * The navigation table is intended to find the correct matrix table for a given branch and keyspace.
 *
 * <p>
 * The navigation table has the following schema:
 *
 * <pre>
 * +-------------+--------------+--------------+--------------+-----------------+-----------------+
 * |             | ID           | Branch       | Keyspace     | MatrixTableName | Time            |
 * +-------------+--------------+--------------+--------------+-----------------+-----------------+
 * | TYPEBOUND   | VARCHAR(255) | VARCHAR(255) | VARCHAR(255) | VARCHAR(255)    | BIGINT          |
 * | CONSTRAINTS | PRIMARY KEY  | NOT NULL     | NOT NULL     | NOT NULL        | NOT NULL        |
 * +-------------+--------------+--------------+--------------+-----------------+-----------------+
 * </pre>
 *
 * There is exactly one instance of this table per {@link ChronoDB}. The name of this table is stored in the constant
 * {@link JdbcNavigationTable#NAME}.
 *
 * <p>
 * This class has <tt>default</tt> visibility (<tt>friendly</tt> visibility) on purpose. It is not intended to be used
 * outside of the package it resides in.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
class JdbcNavigationTable extends DefaultJdbcTable {

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	/**
	 * Grants access to the navigation table, using the given connection.
	 *
	 * @param connection
	 *            The connection to use for communication. Must not be <code>null</code>. Must be open.
	 * @return The navigation table. Never <code>null</code>.
	 */
	public static JdbcNavigationTable get(final Connection connection) {
		return new JdbcNavigationTable(connection);
	}

	// =================================================================================================================
	// TABLE DEFINITION
	// =================================================================================================================

	public static final String NAME = "Navigation";

	public static final String COLUMN_ID = "id";
	public static final String TYPEBOUND_ID = "VARCHAR(255) NOT NULL";

	public static final String COLUMN_BRANCH = "branch";
	public static final String TYPEBOUND_BRANCH = "VARCHAR(255) NOT NULL";

	public static final String COLUMN_KEYSPACE = "keyspace";
	public static final String TYPEBOUND_KEYSPACE = "VARCHAR(255) NOT NULL";

	public static final String COLUMN_MATRIXTABLENAME = "matrixTableName";
	public static final String TYPEBOUND_MATRIXTABLENAME = "VARCHAR(255) NOT NULL";

	public static final String COLUMN_TIMESTAMP = "time";
	public static final String TYPEBOUND_TIMESTAMP = "BIGINT NOT NULL";

	public static final TableColumn[] COLUMNS = {
			//
			new TableColumn(COLUMN_ID, TYPEBOUND_ID),
			//
			new TableColumn(COLUMN_BRANCH, TYPEBOUND_BRANCH),
			//
			new TableColumn(COLUMN_KEYSPACE, TYPEBOUND_KEYSPACE),
			//
			new TableColumn(COLUMN_MATRIXTABLENAME, TYPEBOUND_MATRIXTABLENAME),
			//
			new TableColumn(COLUMN_TIMESTAMP, TYPEBOUND_TIMESTAMP)
	};

	public static final IndexDeclaration[] INDICES = {
			//
			new IndexDeclaration(NAME + "_BranchIndex", COLUMN_BRANCH)
			//
	};

	// =================================================================================================================
	// SQL STATEMENTS
	// =================================================================================================================

	public static final String SQL_GET_BRANCH_NAMES = "SELECT DISTINCT " + COLUMN_BRANCH + " FROM " + NAME;

	public static final String SQL_GET_MATRIX_TABLES_FOR_BRANCH = "SELECT " + COLUMN_MATRIXTABLENAME + " FROM " + NAME + " WHERE " + COLUMN_BRANCH + " = ?";

	public static final String SQL_DELETE_BRANCH = "DELETE FROM " + NAME + " WHERE " + COLUMN_BRANCH + " = ?";

	public static final String SQL_GET_KEYSPACES_AND_MATRIX_TABLES_FOR_BRANCH = "SELECT * FROM " + NAME + " WHERE " + COLUMN_BRANCH + " = ?";

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected JdbcNavigationTable(final Connection connection) {
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

	/**
	 * Inserts a row into the navigation table.
	 *
	 * @param primaryKey
	 *            The id for the new entry. Must not be <code>null</code>. Must be unique in the table.
	 * @param branchName
	 *            The branch name to use. Must not be <code>null</code>.
	 * @param keyspaceName
	 *            The keyspace name to use. Must not be <code>null</code>.
	 * @param matrixTableName
	 *            The name of the matrix table to use. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp at which the matrix was created. Must not be negative.
	 *
	 * @throws ChronoDBStorageBackendException
	 *             Thrown if a backend error occurs during the operation.
	 */
	public void insert(final String primaryKey, final String branchName, final String keyspaceName, final String matrixTableName, final long timestamp) throws ChronoDBStorageBackendException {
		checkNotNull(this.connection, "Precondition violation - argument 'connection' must not be NULL!");
		checkNotNull(primaryKey, "Precondition violation - argument 'primaryKey' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
		checkNotNull(matrixTableName, "Precondition violation - argument 'matrixTableName' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		StringBuilder sql = new StringBuilder();
		sql.append("INSERT INTO ");
		sql.append(NAME);
		sql.append(" VALUES(?, ?, ?, ?, ?)");
		try (PreparedStatement stmt = this.connection.prepareStatement(sql.toString())) {
			stmt.setString(1, primaryKey);
			stmt.setString(2, branchName);
			stmt.setString(3, keyspaceName);
			stmt.setString(4, matrixTableName);
			stmt.setLong(5, timestamp);
			stmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not insert a row into Navigation Table!", e);
		}
	}

	/**
	 * Returns a set of all branch names stored in the navigation table.
	 *
	 * @return The set of branch names. May be empty, but never <code>null</code>.
	 *
	 * @throws ChronoDBStorageBackendException
	 *             Thrown if a backend error occurs during the operation.
	 */
	public Set<String> getBranchNames() throws ChronoDBStorageBackendException {
		try (Statement stmt = this.connection.createStatement()) {
			try (ResultSet resultSet = stmt.executeQuery(SQL_GET_BRANCH_NAMES)) {
				Set<String> branchNames = Sets.newHashSet();
				while (resultSet.next()) {
					branchNames.add(resultSet.getString(1));
				}
				return branchNames;
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Navigation Table!", e);
		}
	}

	/**
	 * Returns the names of all matrix tables associated with the given branch.
	 *
	 * @param branchName
	 *            The name of the branch to get the contained matrix table names for. Must not be <code>null</code>.
	 *
	 * @return The set of matrix table names in the given branch. May be empty, but never <code>null</code>.
	 *
	 * @throws ChronoDBStorageBackendException
	 *             Thrown if a backend error occurs during the operation.
	 */
	public Set<String> getMatrixTableNamesForBranch(final String branchName) throws ChronoDBStorageBackendException {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_GET_MATRIX_TABLES_FOR_BRANCH)) {
			pstmt.setString(1, branchName);
			try (ResultSet resultSet = pstmt.executeQuery()) {
				Set<String> matrixTableNames = Sets.newHashSet();
				while (resultSet.next()) {
					matrixTableNames.add(resultSet.getString(1));
				}
				return matrixTableNames;
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Navigation Table!", e);
		}
	}

	/**
	 * Deletes all entries in the navigation table which point to the given branch.
	 *
	 * @param branchName
	 *            The name of the branch to delete. Must not be <code>null</code>.
	 *
	 * @throws ChronoDBStorageBackendException
	 *             Thrown if a backend error occurs during the operation.
	 */
	public void deleteBranch(final String branchName) throws ChronoDBStorageBackendException {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_DELETE_BRANCH)) {
			pstmt.setString(1, branchName);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Navigation Table!", e);
		}
	}

	/**
	 * Checks if there is at least one matrix table for the given branch.
	 *
	 * @param branchName
	 *            The name of the branch to check. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if there is at least one matrix table for the given branch, otherwise
	 *         <code>false</code>.
	 *
	 * @throws ChronoDBStorageBackendException
	 *             Thrown if a backend error occurs during the operation.
	 */
	public boolean existsBranch(final String branchName) throws ChronoDBStorageBackendException {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_GET_MATRIX_TABLES_FOR_BRANCH)) {
			pstmt.setString(1, branchName);
			try (ResultSet resultSet = pstmt.executeQuery()) {
				if (resultSet.next()) {
					// there is at least one entry in the navigation table
					// that mentions the name of this branch, so it does exist
					return true;
				} else {
					// no entry in the navigation table mentions this branch,
					// so it does not exist
					return false;
				}
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Navigation Table!", e);
		}
	}

	/**
	 * Returns a mapping from keyspace name to the name of the corresponding matrix table name.
	 *
	 * @param branchName
	 *            The name of the branch to get the contained keyspaces for. Must not be <code>null</code>.
	 *
	 * @return The metadata for all known keyspaces in the given branch.
	 *
	 * @throws ChronoDBStorageBackendException
	 *             Thrown if a backend error occurs during the operation.
	 */
	public Set<KeyspaceMetadata> getKeyspaceMetadata(final String branchName) throws ChronoDBStorageBackendException {
		checkNotNull(this.connection, "Precondition violation - argument 'connection' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_GET_KEYSPACES_AND_MATRIX_TABLES_FOR_BRANCH)) {
			pstmt.setString(1, branchName);
			try (ResultSet resultSet = pstmt.executeQuery()) {
				Set<KeyspaceMetadata> metadataSet = Sets.newHashSet();
				while (resultSet.next()) {
					String keyspace = resultSet.getString(COLUMN_KEYSPACE);
					String matrixTableName = resultSet.getString(COLUMN_MATRIXTABLENAME);
					long timestamp = resultSet.getLong(COLUMN_TIMESTAMP);
					KeyspaceMetadata metadata = new KeyspaceMetadata(keyspace, matrixTableName, timestamp);
					metadataSet.add(metadata);
				}
				return metadataSet;
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Navigation Table!", e);
		}
	}

}