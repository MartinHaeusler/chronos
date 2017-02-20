package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.internal.impl.jdbc.table.DefaultJdbcTable;
import org.chronos.chronodb.internal.impl.jdbc.table.IndexDeclaration;
import org.chronos.chronodb.internal.impl.jdbc.table.TableColumn;

import com.google.common.collect.Maps;

/**
 * The index dirty flags table is intended to store the "index name to dirty flag" mapping.
 *
 * <p>
 * The table has the following schema:
 *
 * <pre>
 * +-------------+--------------+--------------+-----------+
 * |             | id           | indexname    | dirtyflag |
 * +-------------+--------------+--------------+-----------+
 * | TYPEBOUND   | VARCHAR(255) | VARCHAR(255) | BOOLEAN   |
 * | CONSTRAINTS | PRIMARY KEY  | NOT NULL     | NOT NULL  |
 * +-------------+--------------+--------------+-----------+
 * </pre>
 *
 * There is exactly one instance of this table per {@link ChronoDB}. The name of this table is stored in the constant
 * {@link JdbcIndexDirtyFlagsTable#NAME}.
 *
 * <p>
 * This class has <tt>default</tt> visibility (<tt>friendly</tt> visibility) on purpose. It is not intended to be used
 * outside of the package it resides in.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
class JdbcIndexDirtyFlagsTable extends DefaultJdbcTable {

	/**
	 * Grants access to the index dirty flag table, using the given connection.
	 *
	 * @param connection
	 *            The connection to use for communication. Must not be <code>null</code>. Must be open.
	 * @return The index dirty flag table. Never <code>null</code>.
	 */
	public static JdbcIndexDirtyFlagsTable get(final Connection connection) {
		return new JdbcIndexDirtyFlagsTable(connection);
	}

	// =================================================================================================================
	// TABLE DEFINITION
	// =================================================================================================================

	public static final String NAME = "ChronoIndexDirtyFlags";

	public static final String PROPERTY_ID = "id";

	public static final String TYPEBOUND_ID = "VARCHAR(255) NOT NULL";

	public static final String PROPERTY_INDEX_NAME = "indexname";

	public static final String TYPEBOUND_INDEX_NAME = "VARCHAR(255) NOT NULL";

	public static final String PROPERTY_DIRTY_FLAG = "dirtyflag";

	public static final String TYPEBOUND_DIRTY_FLAG = "BOOLEAN NOT NULL";

	public static final TableColumn[] COLUMNS = {
			//
			new TableColumn(PROPERTY_ID, TYPEBOUND_ID),
			//
			new TableColumn(PROPERTY_INDEX_NAME, TYPEBOUND_INDEX_NAME),
			//
			new TableColumn(PROPERTY_DIRTY_FLAG, TYPEBOUND_DIRTY_FLAG)
			//
	};

	public static final IndexDeclaration[] INDICES = {
			//
	};

	// =================================================================================================================
	// SQL STATEMENTS
	// =================================================================================================================

	public static final String SQL_INSERT = "INSERT INTO " + NAME + "  VALUES(?, ?, ?)";

	public static final String SQL_UPDATE = "UPDATE " + NAME + " SET " + PROPERTY_DIRTY_FLAG + " = ? WHERE "
			+ PROPERTY_INDEX_NAME + " = ?";

	public static final String SQL_REMOVE_INDEX = "DELETE FROM " + NAME + " WHERE " + PROPERTY_INDEX_NAME + " = ?";

	public static final String SQL_GET_INDEX_STATES = "SELECT " + PROPERTY_INDEX_NAME + ", " + PROPERTY_DIRTY_FLAG
			+ " FROM " + NAME;

	public static final String SQL_GET_INDEX_STATE = "SELECT " + PROPERTY_DIRTY_FLAG + " FROM " + NAME + " WHERE "
			+ PROPERTY_INDEX_NAME + " = ?";

	public static final String SQL_GET_DIRTY_INDEX_NAMES = "SELECT " + PROPERTY_INDEX_NAME + " FROM " + NAME + " WHERE "
			+ PROPERTY_DIRTY_FLAG + " = TRUE";

	public static final String SQL_REMOVE_INDEX_DIRTY_FLAGS = "DELETE FROM " + NAME + " WHERE 1 = 1";

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected JdbcIndexDirtyFlagsTable(final Connection connection) {
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
	 * Inserts a new row into the index flags dirty table with the given properties.
	 *
	 * @param indexName
	 *            The name of the index for which to set the dirty flag. Must not be <code>null</code>.
	 * @param dirty
	 *            The dirty flag value to set for the given index.
	 */
	public void insert(final String indexName, final boolean dirty) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_INSERT)) {
			String id = UUID.randomUUID().toString();
			pstmt.setString(1, id);
			pstmt.setString(2, indexName);
			pstmt.setBoolean(3, dirty);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not perform insert or update in IndexDirtyFlags Table!",
					e);
		}
	}

	/**
	 * Returns states of all indices.
	 *
	 * @return A mapping from index name to dirty state. <code>false</code> indicates that the index is clean,
	 *         <code>true</code> indicates that it is dirty and should be rebuilt.
	 */
	public Map<String, Boolean> getIndexStates() {
		Map<String, Boolean> resultMap = Maps.newHashMap();
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_GET_INDEX_STATES)) {
			try (ResultSet resultSet = pstmt.executeQuery()) {
				while (resultSet.next()) {
					String indexName = resultSet.getString(PROPERTY_INDEX_NAME);
					boolean dirtyFlag = resultSet.getBoolean(PROPERTY_DIRTY_FLAG);
					resultMap.put(indexName, dirtyFlag);
				}
			}
			return resultMap;
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not perform read operation in IndexDirtyFlags Table!", e);
		}
	}

	/**
	 * Removes all entries in the Index Dirty Flag Table.
	 *
	 */
	public void removeIndexDirtyFlags() {
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_REMOVE_INDEX_DIRTY_FLAGS)) {
			pstmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not perform update in IndexDirtyFlags Table!", e);
		}
	}

	/**
	 * Removes the index dirty flag for the given index.
	 *
	 * @param indexName
	 *            The name of the index to clear the dirty flag information for. Must not be <code>null</code>.
	 */
	public void removeIndexDirtyFlag(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_REMOVE_INDEX)) {
			pstmt.setString(1, indexName);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not perform update in IndexDirtyFlags Table!", e);
		}
	}

	/**
	 * Checks the state of the given index.
	 *
	 * @param indexName
	 *            The name of the index to query the state for. Must not be <code>null</code>.
	 * @return <code>false</code> if the index is clean and ready to be used; <code>true</code> if the index is dirty;
	 *         <code>null</code> if there is no information about this index (i.e. the index very likely does not exist
	 *         at all)
	 */
	public Boolean isIndexDirty(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_GET_INDEX_STATE)) {
			pstmt.setString(1, indexName);
			try (ResultSet resultSet = pstmt.executeQuery()) {
				if (resultSet.next() == false) {
					// no data about this index...
					return null;
				}
				boolean dirty = resultSet.getBoolean(PROPERTY_DIRTY_FLAG);
				// if we have ANOTHER result, then clearly something went wrong...
				if (resultSet.next()) {
					throw new ChronoDBStorageBackendException(
							"Found multiple index dirty flags for index '" + indexName + "'!");
				}
				return dirty;
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not query IndexDirtyFlags Table!", e);
		}

	}
}
