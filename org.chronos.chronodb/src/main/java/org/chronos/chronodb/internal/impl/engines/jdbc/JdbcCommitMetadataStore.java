package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.SQLException;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.internal.impl.engines.base.AbstractCommitMetadataStore;

public class JdbcCommitMetadataStore extends AbstractCommitMetadataStore {

	protected JdbcCommitMetadataStore(final JdbcChronoDB owningDB, final Branch owningBranch) {
		super(owningDB, owningBranch);
		this.ensureCommitMetadataTableExists();
	}

	// =====================================================================================================================
	// API IMPLEMENTATION
	// =====================================================================================================================

	@Override
	protected void putInternal(final long commitTimestamp, final byte[] metadata) {
		checkArgument(commitTimestamp >= 0,
				"Precondition violation - argument 'commitTimestamp' must not be negative!");
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		checkArgument(metadata.length > 0,
				"Precondition violation - argument 'metadata' must not be a zero-length array!");
		String branchName = this.getBranchName();
		try (Connection connection = this.openConnection()) {
			JdbcCommitMetadataTable.get(connection).insert(branchName, commitTimestamp, metadata);
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to access Commit Metadata Table!", e);
		}

	}

	@Override
	protected byte[] getInternal(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		String branchName = this.getBranchName();
		try (Connection connection = this.openConnection()) {
			return JdbcCommitMetadataTable.get(connection).getCommitMetadata(branchName, timestamp);
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to access Commit Metadata Table!", e);
		}
	}

	@Override
	protected void rollbackToTimestampInternal(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		String branchName = this.getBranchName();
		try (Connection connection = this.openConnection()) {
			JdbcCommitMetadataTable.get(connection).rollbackBranchToTimestamp(branchName, timestamp);
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to access Commit Metadata Table!", e);
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	@Override
	protected JdbcChronoDB getOwningDB() {
		return (JdbcChronoDB) super.getOwningDB();
	}

	private Connection openConnection() {
		try {
			return this.getOwningDB().getDataSource().getConnection();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not open JDBC Connection!", e);
		}
	}

	private void ensureCommitMetadataTableExists() {
		try (Connection c = this.openConnection()) {
			JdbcCommitMetadataTable commitMetadataTable = JdbcCommitMetadataTable.get(c);
			commitMetadataTable.ensureExists();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to create Commit Metadata Table!");
		}
	}

}
