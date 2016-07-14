package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.internal.impl.jdbc.table.DefaultJdbcTable;
import org.chronos.chronodb.internal.impl.jdbc.table.IndexDeclaration;
import org.chronos.chronodb.internal.impl.jdbc.table.TableColumn;
import org.chronos.chronodb.internal.impl.jdbc.util.JdbcUtils;

/**
 * The time table is intended to store the "now" timestamp for each branch.
 *
 * <p>
 * The time table has the following schema:
 *
 * <pre>
 * +-------------+--------------+----------+
 * |             | Branch       | Now      |
 * +-------------+--------------+----------+
 * | TYPEBOUND   | VARCHAR(255) | BIGINT   |
 * | CONSTRAINTS | PRIMARY KEY  | NOT NULL |
 * +-------------+--------------+----------+
 * </pre>
 *
 * There is exactly one instance of this table per {@link ChronoDB}. The name of this table is stored in the constant
 * {@link JdbcTimeTable#NAME}.
 *
 * <p>
 * This class has <tt>default</tt> visibility (<tt>friendly</tt> visibility) on purpose. It is not intended to be used
 * outside of the package it resides in.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
class JdbcTimeTable extends DefaultJdbcTable {

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	/**
	 * Grants access to the time table, using the given connection.
	 *
	 * @param connection
	 *            The connection to use for communication. Must not be <code>null</code>. Must be open.
	 * @return The time table. Never <code>null</code>.
	 */
	public static JdbcTimeTable get(final Connection connection) {
		return new JdbcTimeTable(connection);
	}

	// =================================================================================================================
	// TABLE DEFINITION
	// =================================================================================================================

	public static final String NAME = "TimeTable";

	public static final String PROPERTY_BRANCH = "branch";
	public static final String TYPEBOUND_BRANCH = "VARCHAR(255) NOT NULL";

	public static final String PROPERTY_NOW = "now";
	public static final String TYPEBOUND_NOW = "BIGINT NOT NULL";

	public static TableColumn[] COLUMNS = {
			//
			new TableColumn(PROPERTY_BRANCH, TYPEBOUND_BRANCH),
			//
			new TableColumn(PROPERTY_NOW, TYPEBOUND_NOW)
			//
	};

	public static IndexDeclaration[] INDICES = {
			// none so far
	};

	// =================================================================================================================
	// SQL STATEMENTS
	// =================================================================================================================

	public static final String SQL_CREATE_STATEMENT = JdbcUtils.renderCreateTableStatement(NAME,
			// Branch name column; primary key
			PROPERTY_BRANCH, TYPEBOUND_BRANCH,
			// Now column
			PROPERTY_NOW, TYPEBOUND_NOW);

	public static final String SQL_INSERT_NOW = "INSERT INTO " + NAME + "  VALUES(?, ?)";

	public static final String SQL_REMOVE_NOW = "DELETE FROM " + NAME + " WHERE " + PROPERTY_BRANCH + " = ?";

	public static final String SQL_GET_NOW = "SELECT " + PROPERTY_NOW + " FROM " + NAME + " WHERE " + PROPERTY_BRANCH
			+ " = ?";

	public static final String SQL_SET_NOW = "UPDATE " + NAME + " SET " + PROPERTY_NOW + " = ? WHERE " + PROPERTY_BRANCH
			+ " = ?";

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected JdbcTimeTable(final Connection connection) {
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
	 * Inserts a row into the time table.
	 *
	 * @param branchName
	 *            The branch name to use. Must not be <code>null</code>. Must be unique in the table.
	 * @param now
	 *            The timestamp that demarcates "now" in this branch. Must not be negative.
	 *
	 * @throws ChronoDBStorageBackendException
	 *             Thrown if a backend error occurs during the operation.
	 */
	public void insert(final String branchName, final long now) throws ChronoDBStorageBackendException {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(now >= 0, "Precondition violation - argument 'now' must be >= 0 (value: " + now + ")!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_INSERT_NOW)) {
			pstmt.setString(1, branchName);
			pstmt.setLong(2, now);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not insert a row into Time Table!", e);
		}
	}

	/**
	 * Removes the row corresponding to the given branch name from the Time Table.
	 *
	 * @param branchName
	 *            The branch name to remove form the Time Table. Must not be <code>null</code>.
	 *
	 * @throws ChronoDBStorageBackendException
	 *             Thrown if a backend error occcurs during the operation.
	 */
	public void delete(final String branchName) throws ChronoDBStorageBackendException {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_REMOVE_NOW)) {
			pstmt.setString(1, branchName);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not remove a row from Time Table!", e);
		}
	}

	/**
	 * Gets the 'now' timestamp for the given branch.
	 *
	 * @param branchName
	 *            The name of the branch to retrieve the timestamp for. Must not be <code>null</code>.
	 *
	 * @return The 'now' timestamp for the given branch. If no entry is found for the given branch name, 0 is returned.
	 */
	public long getNow(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_GET_NOW)) {
			pstmt.setString(1, branchName);
			try (ResultSet resultSet = pstmt.executeQuery()) {
				if (resultSet.next() == false) {
					return 0;
				}
				return resultSet.getLong(1);
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Time Table!", e);
		}
	}

	/**
	 * Sets the 'now' timestamp of the given branch to the specified value.
	 *
	 * @param branchName
	 *            The name of the branch to set the timestamp for. Must not be <code>null</code>.
	 * @param now
	 *            The 'now' timestamp to use. Must be >= 0.
	 */
	public void setNow(final String branchName, final long now) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(now >= 0, "Precondition violation - argument 'now' must be >= 0 (value: " + now + ")!");
		// try it with a UPDATE, if the update affects nothing, perform an insert instead
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_SET_NOW)) {
			pstmt.setLong(1, now);
			pstmt.setString(2, branchName);
			int changedRows = pstmt.executeUpdate();
			if (changedRows <= 0) {
				this.insert(branchName, now);
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Time Table!", e);
		}
	}

}