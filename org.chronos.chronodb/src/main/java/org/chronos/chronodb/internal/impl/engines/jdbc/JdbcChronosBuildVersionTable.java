package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.internal.impl.jdbc.table.DefaultJdbcTable;
import org.chronos.chronodb.internal.impl.jdbc.table.IndexDeclaration;
import org.chronos.chronodb.internal.impl.jdbc.table.TableColumn;
import org.chronos.chronodb.internal.impl.jdbc.util.NamedParameterStatement;

/**
 * The time table is intended to store the commit metadata for all branches.
 *
 * <p>
 * The time table has the following schema:
 *
 * <pre>
 * +-------------+--------------+----------------+
 * |             | ID           | chronosVersion |
 * +-------------+--------------+----------------+
 * | TYPEBOUND   | VARCHAR(255) | VARCHAR(255)   |
 * | CONSTRAINTS | PRIMARY KEY  | NOT NULL       |
 * +-------------+--------------+----------------+
 * </pre>
 *
 * There is exactly one instance of this table per {@link ChronoDB}. The name of this table is stored in the constant
 * {@link JdbcChronosBuildVersionTable#NAME}.
 *
 * <p>
 * This class has <tt>default</tt> visibility (<tt>friendly</tt> visibility) on purpose. It is not intended to be used
 * outside of the package it resides in.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
/* default */ class JdbcChronosBuildVersionTable extends DefaultJdbcTable {

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	/**
	 * Grants access to the Chronos Build Version table, using the given connection.
	 *
	 * @param connection
	 *            The connection to use for communication. Must not be <code>null</code>. Must be open.
	 * @return The build version table. Never <code>null</code>.
	 */
	public static JdbcChronosBuildVersionTable get(final Connection connection) {
		return new JdbcChronosBuildVersionTable(connection);
	}

	// =================================================================================================================
	// TABLE DEFINITION
	// =================================================================================================================

	public static final String NAME = "ChronosVersionTable";

	public static final String PROPERTY_ID = "ID";
	public static final String TYPEBOUND_ID = "VARCHAR(255) NOT NULL";

	public static final String PROPERTY_CHRONOS_VERSION = "chronosVersion";
	public static final String TYPEBOUND_CHRONOS_VERSION = "VARCHAR(255) NOT NULL";

	public static TableColumn[] COLUMNS = {
			//
			new TableColumn(PROPERTY_ID, TYPEBOUND_ID),
			//
			new TableColumn(PROPERTY_CHRONOS_VERSION, TYPEBOUND_CHRONOS_VERSION)
			//
	};

	public static IndexDeclaration[] INDICES = {
			// none so far
	};

	// =====================================================================================================================
	// SQL STATEMENTS
	// =====================================================================================================================

	public static final String NAMED_SQL__INSERT = "INSERT INTO " + NAME + " VALUES(${id}, ${chronosVersion})";

	public static final String SQL__DELETE_TABLE_CONTENTS = "DELETE FROM " + NAME + " WHERE 1 = 1";

	public static final String SQL__GET_CHRONOS_VERSION = "SELECT " + PROPERTY_CHRONOS_VERSION + " FROM " + NAME
			+ " WHERE 1 = 1";

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected JdbcChronosBuildVersionTable(final Connection connection) {
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

	public void setChronosVersion(final String chronosVersion) {
		checkNotNull(chronosVersion, "Precondition violation - argument 'chronosVersion' must not be NULL!");
		try {
			try (PreparedStatement pStmt = this.connection.prepareStatement(SQL__DELETE_TABLE_CONTENTS)) {
				pStmt.executeUpdate();
			}
			try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, NAMED_SQL__INSERT)) {
				nStmt.setParameter("id", UUID.randomUUID().toString());
				nStmt.setParameter("chronosVersion", chronosVersion);
				nStmt.executeUpdate();
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to update Chronos Build Version Table!", e);
		}
	}

	public String getChronosVersion() {
		try {
			try (PreparedStatement pStmt = this.connection.prepareStatement(SQL__GET_CHRONOS_VERSION)) {
				try (ResultSet resultSet = pStmt.executeQuery()) {
					if (resultSet.next() == false) {
						return null;
					}
					String chronosVersion = resultSet.getString(PROPERTY_CHRONOS_VERSION);
					if (resultSet.next()) {
						throw new IllegalStateException("Found multiple entries in Chronos Build Version Table!");
					}
					return chronosVersion;
				}
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to read from Chronos Build Version Table!", e);
		}

	}
}
