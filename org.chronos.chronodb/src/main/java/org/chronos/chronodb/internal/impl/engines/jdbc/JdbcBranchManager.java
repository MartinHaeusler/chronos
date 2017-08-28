package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.BranchManager;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.api.exceptions.JdbcTableException;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.impl.BranchImpl;
import org.chronos.chronodb.internal.impl.IBranchMetadata;
import org.chronos.chronodb.internal.impl.engines.base.AbstractBranchManager;
import org.chronos.common.logging.ChronoLogger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class JdbcBranchManager extends AbstractBranchManager implements BranchManager {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final Map<String, BranchInternal> loadedBranches = Maps.newConcurrentMap();
	private final Map<String, IBranchMetadata> branchMetadata = Maps.newConcurrentMap();

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected JdbcBranchManager(final JdbcChronoDB owningDb) {
		super(owningDb);
		this.ensureBranchingTablesExist();
		this.loadBranchMetadata();
		this.ensureMasterBranchExists();
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public Set<String> getBranchNames() {
		return Collections.unmodifiableSet(Sets.newHashSet(this.branchMetadata.keySet()));
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	@Override
	protected BranchInternal createBranch(final IBranchMetadata metadata) {
		BranchInternal parentBranch = null;
		BranchImpl branch = null;
		if (metadata.getParentName() != null) {
			// regular branch
			parentBranch = (BranchInternal) this.getBranch(metadata.getParentName());
			branch = BranchImpl.createBranch(metadata, parentBranch);
		} else {
			// master branch
			parentBranch = null;
			branch = BranchImpl.createMasterBranch();
		}
		try (Connection connection = this.openConnection()) {
			JdbcNavigationTable navTable = JdbcNavigationTable.get(connection);
			String primaryKey = UUID.randomUUID().toString();
			String keyspaceName = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;
			String tableName = JdbcMatrixTable.generateRandomName();
			ChronoLogger.logTrace("Creating branch: [" + primaryKey + ", " + branch.getName() + ", " + keyspaceName
					+ ", " + tableName + "]");
			navTable.insert(primaryKey, branch.getName(), keyspaceName, tableName, 0L);
			JdbcBranchMetadataTable.get(connection).insertOrUpdate(metadata);
			connection.commit();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Navigation Table!", e);
		}
		this.attachTKVS(branch);
		this.loadedBranches.put(branch.getName(), branch);
		this.branchMetadata.put(branch.getName(), branch.getMetadata());
		return branch;
	}

	@Override
	protected BranchInternal getBranchInternal(final String name) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		BranchInternal branch = this.loadedBranches.get(name);
		if (branch != null) {
			// already loaded
			return branch;
		}
		// not loaded yet; load it
		IBranchMetadata metadata = this.branchMetadata.get(name);
		if (metadata == null) {
			return null;
		}
		if (metadata.getParentName() == null) {
			// we are the master branch
			branch = BranchImpl.createMasterBranch();
		} else {
			Branch parentBranch = this.getBranch(metadata.getParentName());
			branch = BranchImpl.createBranch(metadata, parentBranch);
		}
		// attach the TKVS to the branch
		this.attachTKVS(branch);
		this.loadedBranches.put(branch.getName(), branch);
		return branch;
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private JdbcChronoDB getOwningDB() {
		return (JdbcChronoDB) this.owningDb;
	}

	private Connection openConnection() throws SQLException {
		return this.getOwningDB().getDataSource().getConnection();
	}

	/**
	 * Ensures the existence of the navigation table in the database.
	 */
	private void ensureBranchingTablesExist() {
		try (Connection connection = this.openConnection()) {
			JdbcNavigationTable.get(connection).ensureExists();
			JdbcBranchMetadataTable.get(connection).ensureExists();
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Could not create Navigation Table and/or Branch Metadata Table!",
					e);
		}
	}

	private void ensureMasterBranchExists() {
		IBranchMetadata masterBranchMetadata = IBranchMetadata.createMasterBranchMetadata();
		if (this.existsBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER)) {
			// we know that the master branch exists in our navigation map.
			// ensure that it also exists in the branch metadata map
			try (Connection connection = this.openConnection()) {
				JdbcBranchMetadataTable.get(connection).insertOrUpdate(masterBranchMetadata);
			} catch (SQLException e) {
				throw new ChronoDBStorageBackendException("Failed to insert/update Branch Metadata table!");
			}
			return;
		}
		this.createBranch(masterBranchMetadata);
	}

	private void loadBranchMetadata() {
		try (Connection connection = this.openConnection()) {
			Set<IBranchMetadata> allMetadata = JdbcBranchMetadataTable.get(connection).getAll();
			for (IBranchMetadata metadata : allMetadata) {
				this.branchMetadata.put(metadata.getName(), metadata);
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to read from Branch Metadata table!");
		}
	}

	private JdbcTkvs attachTKVS(final BranchInternal branch) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		JdbcTkvs branchTKVS = new JdbcTkvs(this.getOwningDB(), branch);
		return branchTKVS;
	}
}
