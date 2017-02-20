package org.chronos.chronodb.internal.impl.engines.tupl;

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

public class TuplTkvs extends AbstractTemporalKeyValueStore {

	private static final String MANAGEMENT_INDEX__WRITE_AHEAD_LOG = "chronodb.wal";

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private long now;
	private final CommitMetadataStore commitMetadataStore;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected TuplTkvs(final TuplChronoDB owningDB, final BranchInternal owningBranch) {
		super(owningDB, owningBranch);
		this.initializeBranch();
		this.now = this.loadNowTimestampFromDB();
		this.initializeKeyspaceToMatrixMapFromDB();
		this.commitMetadataStore = new TuplCommitMetadataStore(owningDB, owningBranch);
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public TuplChronoDB getOwningDB() {
		return (TuplChronoDB) super.getOwningDB();
	}

	@Override
	protected CommitMetadataStore getCommitMetadataStore() {
		return this.commitMetadataStore;
	}

	// =====================================================================================================================
	// INTERNAL API
	// =====================================================================================================================

	@Override
	protected long getNowInternal() {
		return this.now;
	}

	@Override
	protected void setNow(final long timestamp) {
		try (DefaultTuplTransaction tx = this.getOwningDB().openTransaction()) {
			TimeIndex.put(tx, this.getBranchName(), timestamp);
			this.now = timestamp;
			tx.commit();
		}
	}

	@Override
	protected TemporalTuplMatrix getMatrix(final String keyspace) {
		return (TemporalTuplMatrix) this.keyspaceToMatrix.get(keyspace);
	}

	@Override
	protected TemporalTuplMatrix createMatrix(final String keyspace, final long timestamp) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		String matrixTableName = MatrixUtils.generateRandomName();
		try (DefaultTuplTransaction tx = this.getOwningDB().openTransaction()) {
			NavigationIndex.insert(tx, this.getBranchName(), keyspace, matrixTableName, timestamp);
			TemporalTuplMatrix matrix = new TemporalTuplMatrix(keyspace, timestamp, this.getOwningDB(),
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
		return this.getMatrix(keyspace).getIndexName();
	}

	private void initializeKeyspaceToMatrixMapFromDB() {
		try (DefaultTuplTransaction tx = this.getOwningDB().openTransaction()) {
			Set<KeyspaceMetadata> allKeyspaceMetadata = NavigationIndex.getKeyspaceMetadata(tx, this.getBranchName());
			for (KeyspaceMetadata keyspaceMetadata : allKeyspaceMetadata) {
				String keyspace = keyspaceMetadata.getKeyspaceName();
				String matrixTableName = keyspaceMetadata.getMatrixTableName();
				long timestamp = keyspaceMetadata.getCreationTimestamp();
				TemporalTuplMatrix matrix = new TemporalTuplMatrix(keyspace, timestamp, this.getOwningDB(),
						matrixTableName);
				this.keyspaceToMatrix.put(keyspace, matrix);
				logTrace("Registering keyspace '" + keyspace + "' matrix in branch '" + this.getBranchName() + "': "
						+ matrixTableName);
			}
		}
	}

	private long loadNowTimestampFromDB() {
		try (DefaultTuplTransaction tx = this.getOwningDB().openTransaction()) {
			return TimeIndex.get(tx, this.getBranchName());
		}
	}

	private void initializeBranch() {
		try (DefaultTuplTransaction tx = this.getOwningDB().openTransaction()) {
			if (NavigationIndex.existsBranch(tx, this.getBranchName())) {
				logTrace("Branch '" + this.getBranchName() + "' already exists.");
				return;
			}
			String keyspaceName = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;
			String tableName = MatrixUtils.generateRandomName();
			logTrace("Creating branch: [" + this.getBranchName() + ", " + keyspaceName + ", " + tableName + "]");
			NavigationIndex.insert(tx, this.getBranchName(), keyspaceName, tableName, 0L);
			tx.commit();
		}
	}

	@Override
	protected void performWriteAheadLog(final WriteAheadLogToken token) {
		try (DefaultTuplTransaction tx = this.getOwningDB().openTransaction()) {
			byte[] serializedToken = this.getOwningDB().getSerializationManager().serialize(token);
			tx.store(TuplChronoDB.MANAGEMENT_INDEX_NAME, MANAGEMENT_INDEX__WRITE_AHEAD_LOG, serializedToken);
			tx.commit();
		}
	}

	@Override
	protected void clearWriteAheadLogToken() {
		try (DefaultTuplTransaction tx = this.getOwningDB().openTransaction()) {
			tx.delete(TuplChronoDB.MANAGEMENT_INDEX_NAME, MANAGEMENT_INDEX__WRITE_AHEAD_LOG);
			tx.commit();
		}
	}

	@Override
	protected WriteAheadLogToken getWriteAheadLogTokenIfExists() {
		try (DefaultTuplTransaction tx = this.getOwningDB().openTransaction()) {
			byte[] serializedToken = tx.load(TuplChronoDB.MANAGEMENT_INDEX_NAME, MANAGEMENT_INDEX__WRITE_AHEAD_LOG);
			if (serializedToken == null || serializedToken.length <= 0) {
				return null;
			}
			WriteAheadLogToken token = (WriteAheadLogToken) this.getOwningDB().getSerializationManager()
					.deserialize(serializedToken);
			return token;
		}
	}
}
