package org.chronos.chronodb.internal.impl.engines.mapdb;

import static org.chronos.common.logging.ChronoLogger.*;

import static com.google.common.base.Preconditions.*;

import java.util.Set;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.CommitMetadataStore;
import org.chronos.chronodb.internal.impl.MatrixUtils;
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalKeyValueStore;
import org.chronos.chronodb.internal.impl.engines.base.KeyspaceMetadata;
import org.chronos.chronodb.internal.impl.engines.base.WriteAheadLogToken;
import org.chronos.chronodb.internal.impl.mapdb.MapDBTransaction;
import org.chronos.chronodb.internal.impl.mapdb.NavigationMap;
import org.chronos.chronodb.internal.impl.mapdb.TimeMap;
import org.mapdb.Atomic.Var;

public class MapDBTkvs extends AbstractTemporalKeyValueStore {

	private static final String WRITE_AHEAD_LOG_VAR_NAME = "chronodb.wal";

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private long now;
	private final CommitMetadataStore commitMetadataStore;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public MapDBTkvs(final MapDBChronoDB db, final BranchInternal branch) {
		super(db, branch);
		this.initializeBranch();
		this.now = this.loadNowTimestampFromDB();
		this.initializeKeyspaceToMatrixMapFromDB();
		this.commitMetadataStore = new MapDBCommitMetadataStore(db, branch);
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public MapDBChronoDB getOwningDB() {
		return (MapDBChronoDB) super.getOwningDB();
	}

	@Override
	protected CommitMetadataStore getCommitMetadataStore() {
		return this.commitMetadataStore;
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
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			TimeMap.put(tx, this.getBranchName(), timestamp);
			this.now = timestamp;
			tx.commit();
		}
	}

	@Override
	protected TemporalMapDBMatrix getMatrix(final String keyspace) {
		return (TemporalMapDBMatrix) this.keyspaceToMatrix.get(keyspace);
	}

	@Override
	protected TemporalMapDBMatrix createMatrix(final String keyspace, final long timestamp) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		String matrixTableName = MatrixUtils.generateRandomName();
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			NavigationMap.insert(tx, this.getBranchName(), keyspace, matrixTableName, timestamp);
			TemporalMapDBMatrix matrix = new TemporalMapDBMatrix(keyspace, timestamp, this.getOwningDB(),
					matrixTableName);
			this.keyspaceToMatrix.put(keyspace, matrix);
			tx.commit();
			return matrix;
		}
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	protected String getBranchName() {
		return this.getOwningBranch().getName();
	}

	protected String getKeyspaceMatrixTableName(final String keyspace) {
		return this.getMatrix(keyspace).getMapName();
	}

	private void initializeKeyspaceToMatrixMapFromDB() {
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			Set<KeyspaceMetadata> allKeyspaceMetadata = NavigationMap.getKeyspaceMetadata(tx, this.getBranchName());
			for (KeyspaceMetadata keyspaceMetadata : allKeyspaceMetadata) {
				String keyspace = keyspaceMetadata.getKeyspaceName();
				String matrixTableName = keyspaceMetadata.getMatrixTableName();
				long timestamp = keyspaceMetadata.getCreationTimestamp();
				TemporalMapDBMatrix matrix = new TemporalMapDBMatrix(keyspace, timestamp, this.getOwningDB(),
						matrixTableName);
				this.keyspaceToMatrix.put(keyspace, matrix);
				logTrace("Registering keyspace '" + keyspace + "' matrix in branch '" + this.getBranchName() + "': "
						+ matrixTableName);
			}
		}
	}

	private long loadNowTimestampFromDB() {
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			long timestamp = TimeMap.get(tx, this.getBranchName());
			return timestamp;
		}
	}

	private void initializeBranch() {
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			if (NavigationMap.existsBranch(tx, this.getBranchName())) {
				logTrace("Branch '" + this.getBranchName() + "' already exists.");
				return;
			}
			String keyspaceName = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;
			String tableName = MatrixUtils.generateRandomName();
			logTrace("Creating branch: [" + this.getBranchName() + ", " + keyspaceName + ", " + tableName + "]");
			NavigationMap.insert(tx, this.getBranchName(), keyspaceName, tableName, 0L);
			tx.commit();
		}
	}

	@Override
	protected void performWriteAheadLog(final WriteAheadLogToken token) {
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			Var<byte[]> var = tx.atomicVar(this.getBranchName() + "." + WRITE_AHEAD_LOG_VAR_NAME);
			var.set(this.getOwningDB().getSerializationManager().serialize(token));
			tx.commit();
		}
	}

	@Override
	protected void clearWriteAheadLogToken() {
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			tx.delete(this.getBranchName() + "." + WRITE_AHEAD_LOG_VAR_NAME);
			tx.commit();
		}
	}

	@Override
	protected WriteAheadLogToken getWriteAheadLogTokenIfExists() {
		String varName = this.getBranchName() + "." + WRITE_AHEAD_LOG_VAR_NAME;
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			if (tx.exists(varName) == false) {
				// variable that holds the WAL token doesn't exist -> no WAL token exists
				return null;
			}
			Var<byte[]> var = tx.atomicVar(varName);
			if (var.get() == null) {
				// variable exists, but contains NULL -> no WAL token exists
				return null;
			}
			byte[] contents = var.get();
			WriteAheadLogToken token = (WriteAheadLogToken) this.getOwningDB().getSerializationManager()
					.deserialize(contents);
			return token;
		}
	}

}
