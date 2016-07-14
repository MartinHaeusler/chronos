package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.internal.impl.BranchMetadata;
import org.chronos.chronodb.internal.impl.jdbc.table.DefaultJdbcTable;
import org.chronos.chronodb.internal.impl.jdbc.table.IndexDeclaration;
import org.chronos.chronodb.internal.impl.jdbc.table.TableColumn;
import org.chronos.chronodb.internal.impl.jdbc.util.NamedParameterStatement;

import com.google.common.collect.Sets;

/**
 * The branch metadata table is intended to store the metadata for each branch.
 *
 * <p>
 * The branch metadata table has the following schema:
 *
 * <pre>
 * +-------------+--------------+--------------+--------------+--------------------+
 * |             | id           | branch       | parentBranch | branchingTimestamp |
 * +-------------+--------------+--------------+--------------+--------------------+
 * | TYPEBOUND   | VARCHAR(255) | VARCHAR(255) | VARCHAR(255) | BIGINT             |
 * | CONSTRAINTS | PRIMARY KEY  | NOT NULL     |              | NOT NULL           |
 * +-------------+--------------+--------------+--------------+--------------------+
 * </pre>
 *
 * There is exactly one instance of this table per {@link ChronoDB}. The name of this table is stored in the constant
 * {@link JdbcBranchMetadataTable#NAME}.
 *
 * <p>
 * This class has <tt>default</tt> visibility (<tt>friendly</tt> visibility) on purpose. It is not intended to be used
 * outside of the package it resides in.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class JdbcBranchMetadataTable extends DefaultJdbcTable {

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	/**
	 * Grants access to the branch metadata table, using the given connection.
	 *
	 * @param connection
	 *            The connection to use for communication. Must not be <code>null</code>. Must be open.
	 * @return The branch metadata table. Never <code>null</code>.
	 */
	public static JdbcBranchMetadataTable get(final Connection connection) {
		return new JdbcBranchMetadataTable(connection);
	}

	// =================================================================================================================
	// TABLE DEFINITION
	// =================================================================================================================

	public static final String NAME = "ChronoBranchMetadata";

	public static final String PROPERTY_ID = "id";

	public static final String TYPEBOUND_ID = "VARCHAR(255) NOT NULL";

	public static final String PROPERTY_BRANCH_NAME = "branch";

	public static final String TYPEBOUND_BRANCH_NAME = "VARCHAR(255) NOT NULL";

	public static final String PROPERTY_PARENT_BRANCH_NAME = "parentBranch";

	public static final String TYPEBOUND_PARENT_BRANCH_NAME = "VARCHAR(255)";

	public static final String PROPERTY_BRANCHING_TIMESTAMP = "branchingTimestamp";

	public static final String TYPEBOUND_BRANCHING_TIMESTAMP = "BIGINT NOT NULL";

	public static final TableColumn[] COLUMNS = {
			//
			new TableColumn(PROPERTY_ID, TYPEBOUND_ID),
			//
			new TableColumn(PROPERTY_BRANCH_NAME, TYPEBOUND_BRANCH_NAME),
			//
			new TableColumn(PROPERTY_PARENT_BRANCH_NAME, TYPEBOUND_PARENT_BRANCH_NAME),
			//
			new TableColumn(PROPERTY_BRANCHING_TIMESTAMP, TYPEBOUND_BRANCHING_TIMESTAMP)
			//
	};

	public static final IndexDeclaration[] INDICES = {
			//
			new IndexDeclaration(NAME + "_BranchIndex", PROPERTY_BRANCH_NAME),
			//
			new IndexDeclaration(NAME + "_ParentBranchIndex", PROPERTY_PARENT_BRANCH_NAME)
			//
	};

	// =================================================================================================================
	// SQL STATEMENTS
	// =================================================================================================================

	public static final String NAMED_SQL_INSERT = "INSERT INTO " + NAME + "  VALUES(${" + PROPERTY_ID + "}, ${"
			+ PROPERTY_BRANCH_NAME + "}, ${" + PROPERTY_PARENT_BRANCH_NAME + "}, ${" + PROPERTY_BRANCHING_TIMESTAMP
			+ "})";

	public static final String NAMED_SQL_REMOVE_BRANCH = "DELETE FROM " + NAME + " WHERE " + PROPERTY_BRANCH_NAME
			+ " = ${" + PROPERTY_BRANCH_NAME + "}";

	public static final String SQL_SELECT_ALL = "SELECT * FROM " + NAME;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected JdbcBranchMetadataTable(final Connection connection) {
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

	public void insertOrUpdate(final BranchMetadata metadata) {
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		String sql;
		sql = NAMED_SQL_REMOVE_BRANCH;
		// first, try to remove the old entry by name
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter(PROPERTY_BRANCH_NAME, metadata.getName());
			nStmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to update Branch Metadata table!");
		}
		sql = NAMED_SQL_INSERT;
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter(PROPERTY_ID, UUID.randomUUID().toString());
			nStmt.setParameter(PROPERTY_BRANCH_NAME, metadata.getName());
			nStmt.setParameter(PROPERTY_PARENT_BRANCH_NAME, metadata.getParentName());
			nStmt.setParameter(PROPERTY_BRANCHING_TIMESTAMP, metadata.getBranchingTimestamp());
			nStmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to insert into Branch Metadata table!", e);
		}
	}

	public Set<BranchMetadata> getAll() {
		String sql = SQL_SELECT_ALL;
		try (PreparedStatement pStmt = this.connection.prepareStatement(sql)) {
			try (ResultSet resultSet = pStmt.executeQuery()) {
				Set<BranchMetadata> allMetadata = Sets.newHashSet();
				while (resultSet.next()) {
					String branchName = resultSet.getString(PROPERTY_BRANCH_NAME);
					String parentName = resultSet.getString(PROPERTY_PARENT_BRANCH_NAME);
					long branchingTimestamp = resultSet.getLong(PROPERTY_BRANCHING_TIMESTAMP);
					BranchMetadata metadata = new BranchMetadata(branchName, parentName, branchingTimestamp);
					allMetadata.add(metadata);
				}
				return allMetadata;
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to read from Branch Metadata table!", e);
		}
	}
}
