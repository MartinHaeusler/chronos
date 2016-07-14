package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.internal.impl.jdbc.table.DefaultJdbcTable;
import org.chronos.chronodb.internal.impl.jdbc.table.IndexDeclaration;
import org.chronos.chronodb.internal.impl.jdbc.table.TableColumn;

class JdbcWALTokenTable extends DefaultJdbcTable {

	/**
	 * Grants access to the WAL Token table, using the given connection.
	 *
	 * @param connection
	 *            The connection to use for communication. Must not be <code>null</code>. Must be open.
	 * @return The WAL Token table. Never <code>null</code>.
	 */
	public static JdbcWALTokenTable get(final Connection connection) {
		return new JdbcWALTokenTable(connection);
	}

	// =================================================================================================================
	// TABLE DEFINITION
	// =================================================================================================================

	public static final String NAME = "ChronoWALTokens";

	public static final String PROPERTY_BRANCH_NAME = "branch";

	public static final String TYPEBOUND_BRANCH_NAME = "VARCHAR(255) NOT NULL";

	public static final String PROPERTY_TOKEN_CONTENT = "tokenContent";

	public static final String TYPEBOUND_TOKEN_CONTENT = "VARBINARY(MAX)";

	public static final TableColumn[] COLUMNS = {
			//
			new TableColumn(PROPERTY_BRANCH_NAME, TYPEBOUND_BRANCH_NAME),
			//
			new TableColumn(PROPERTY_TOKEN_CONTENT, TYPEBOUND_TOKEN_CONTENT)
			//
	};

	public static final IndexDeclaration[] INDICES = {
			//
	};

	// =================================================================================================================
	// SQL QUERIES
	// =================================================================================================================

	public static final String SQL_INSERT = "INSERT INTO " + NAME + "  VALUES(?, ?)";

	public static final String SQL_GET_TOKEN = "SELECT " + PROPERTY_TOKEN_CONTENT + " FROM " + NAME + " WHERE " + PROPERTY_BRANCH_NAME + " = ?";

	public static final String SQL_DELETE_TOKEN = "DELETE FROM " + NAME + " WHERE " + PROPERTY_BRANCH_NAME + " = ?";

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected JdbcWALTokenTable(final Connection connection) {
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

	public void insertToken(final String branchName, final byte[] tokenContent) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(tokenContent, "Precondition violation - argument 'tokenContent' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_INSERT)) {
			Blob blob = this.connection.createBlob();
			blob.setBytes(1, tokenContent);
			// fill the parameters of the prepared statement
			pstmt.setString(1, branchName);
			pstmt.setBlob(2, blob);
			pstmt.executeUpdate();
			pstmt.close();
			blob.free();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not perform insert or update in WALToken Table!", e);
		}
	}

	public byte[] getToken(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_GET_TOKEN)) {
			// fill the parameters of the prepared statement
			pstmt.setString(1, branchName);
			try (ResultSet resultSet = pstmt.executeQuery()) {
				if (resultSet.next() == false) {
					return null;
				}
				Blob blob = resultSet.getBlob(1);
				byte[] bytes = null;
				try {
					bytes = blob.getBytes(1, (int) blob.length());
				} finally {
					blob.free();
				}
				return bytes;
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not perform read on WALToken Table!", e);
		}
	}

	public boolean existsToken(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_GET_TOKEN)) {
			// fill the parameters of the prepared statement
			pstmt.setString(1, branchName);
			try (ResultSet resultSet = pstmt.executeQuery()) {
				if (resultSet.next()) {
					return true;
				} else {
					return false;
				}
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not perform read on WALToken Table!", e);
		}
	}

	public void deleteToken(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_DELETE_TOKEN)) {
			// fill the parameters of the prepared statement
			pstmt.setString(1, branchName);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not perform insert or update in WALToken Table!", e);
		}
	}

}
