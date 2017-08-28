package org.chronos.chronodb.internal.impl.engines.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;

import javax.sql.DataSource;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.CommitMetadataStore;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalKeyValueStore;
import org.chronos.chronodb.internal.impl.engines.base.KeyspaceMetadata;
import org.chronos.chronodb.internal.impl.engines.base.WriteAheadLogToken;
import org.chronos.common.logging.ChronoLogger;

public class JdbcTkvs extends AbstractTemporalKeyValueStore implements TemporalKeyValueStore {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private long now;

	private final JdbcCommitMetadataStore commitMetadataStore;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public JdbcTkvs(final JdbcChronoDB chronoDB, final BranchInternal branch) {
		super(chronoDB, branch);
		this.ensureTimeTableExists();
		this.ensureWALTableExists();
		this.now = this.loadNowTimestampFromDB();
		this.initializeKeyspaceToMatrixMapFromDB();
		this.ensureDefaultKeyspaceExists();
		this.commitMetadataStore = new JdbcCommitMetadataStore(chronoDB, branch);
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	@Override
	protected long getNowInternal() {
		return this.now;
	}

	@Override
	protected void setNow(final long timestamp) {
		try (Connection connection = this.openConnection()) {
			JdbcTimeTable.get(connection).setNow(this.getBranchName(), timestamp);
			this.now = timestamp;
			connection.commit();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Time Table!", e);
		}
	}

	@Override
	protected TemporalJdbcMatrix getMatrix(final String keyspace) {
		return (TemporalJdbcMatrix) super.getMatrix(keyspace);
	}

	@Override
	protected TemporalJdbcMatrix createMatrix(final String keyspace, final long timestamp) {
		String primaryKey = UUID.randomUUID().toString();
		String matrixTableName = JdbcMatrixTable.generateRandomName();
		try (Connection connection = this.openConnection()) {
			ChronoLogger.logTrace("Creating keyspace: [" + primaryKey + ", " + this.getBranchName() + ", " + keyspace
					+ ", " + matrixTableName + "]");
			JdbcNavigationTable.get(connection).insert(primaryKey, this.getBranchName(), keyspace, matrixTableName,
					timestamp);
			DataSource dataSource = this.getOwningDB().getDataSource();
			TemporalJdbcMatrix matrix = new TemporalJdbcMatrix(keyspace, timestamp, dataSource, matrixTableName);
			this.keyspaceToMatrix.put(keyspace, matrix);
			connection.commit();
			return matrix;
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Navigation Table!", e);
		}
	}

	@Override
	public JdbcChronoDB getOwningDB() {
		return (JdbcChronoDB) super.getOwningDB();
	}

	@Override
	protected void performWriteAheadLog(final WriteAheadLogToken token) {
		try (Connection connection = this.openConnection()) {
			byte[] tokenContent = this.getOwningDB().getSerializationManager().serialize(token);
			JdbcWALTokenTable.get(connection).insertToken(this.getBranchName(), tokenContent);
			connection.commit();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to store write-ahead-log token!", e);
		}
	}

	@Override
	protected void clearWriteAheadLogToken() {
		try (Connection connection = this.openConnection()) {
			JdbcWALTokenTable.get(connection).deleteToken(this.getBranchName());
			connection.commit();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to delete write-ahead-log token!", e);
		}
	}

	@Override
	protected WriteAheadLogToken getWriteAheadLogTokenIfExists() {
		try (Connection connection = this.openConnection()) {
			byte[] serializedToken = JdbcWALTokenTable.get(connection).getToken(this.getBranchName());
			if (serializedToken == null) {
				return null;
			}
			return (WriteAheadLogToken) this.getOwningDB().getSerializationManager().deserialize(serializedToken);
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to read the write-ahead-log token!", e);
		}
	}

	@Override
	public CommitMetadataStore getCommitMetadataStore() {
		return this.commitMetadataStore;
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	private String getBranchName() {
		return this.getOwningBranch().getName();
	}

	private Connection openConnection() {
		try {
			return this.getOwningDB().getDataSource().getConnection();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not open JDBC Connection!", e);
		}
	}

	protected String getKeyspaceMatrixTableName(final String keyspace) {
		return this.getMatrix(keyspace).getTableName();
	}

	private void ensureTimeTableExists() {
		try (Connection connection = this.openConnection()) {
			JdbcTimeTable.get(connection).ensureExists();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Time Table!", e);
		}
	}

	private void ensureWALTableExists() {
		try (Connection connection = this.openConnection()) {
			JdbcWALTokenTable.get(connection).ensureExists();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Time Table!", e);
		}
	}

	private long loadNowTimestampFromDB() {
		try (Connection connection = this.openConnection()) {
			return JdbcTimeTable.get(connection).getNow(this.getBranchName());
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Time Table!", e);
		}
	}

	private void initializeKeyspaceToMatrixMapFromDB() {
		try (Connection c = this.openConnection()) {
			JdbcNavigationTable navTable = JdbcNavigationTable.get(c);
			Set<KeyspaceMetadata> allKeyspaceMetadata = navTable.getKeyspaceMetadata(this.getBranchName());
			for (KeyspaceMetadata metadata : allKeyspaceMetadata) {
				String keyspace = metadata.getKeyspaceName();
				DataSource dataSource = this.getOwningDB().getDataSource();
				String matrixTableName = metadata.getMatrixTableName();
				long timestamp = metadata.getCreationTimestamp();
				TemporalJdbcMatrix matrix = new TemporalJdbcMatrix(keyspace, timestamp, dataSource, matrixTableName);
				this.keyspaceToMatrix.put(keyspace, matrix);
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not access Navigation Table!", e);
		}
	}

	private void ensureDefaultKeyspaceExists() {
		if (this.keyspaceToMatrix.containsKey(ChronoDBConstants.DEFAULT_KEYSPACE_NAME)) {
			// default keyspace exists
			return;
		}
		this.createMatrix(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, 0L);
	}

}
