package org.chronos.chronodb.internal.impl.engines.chunkdb;

import static org.chronos.common.logging.ChronoLogger.*;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.CommitMetadataStore;
import org.chronos.chronodb.internal.api.TemporalDataMatrix;
import org.chronos.chronodb.internal.impl.MatrixUtils;
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalKeyValueStore;
import org.chronos.chronodb.internal.impl.engines.base.KeyspaceMetadata;
import org.chronos.chronodb.internal.impl.engines.base.WriteAheadLogToken;
import org.chronos.chronodb.internal.impl.engines.tupl.NavigationIndex;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplDataMatrixUtil;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplUtils;
import org.chronos.chronodb.internal.impl.mapdb.MapDBTransaction;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.chronos.common.exceptions.ChronosIOException;
import org.mapdb.Serializer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ChunkDbTkvs extends AbstractTemporalKeyValueStore {

	private final CommitMetadataStore commitMetadataStore;
	private Long cachedNowTimestamp = null;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected ChunkDbTkvs(final ChunkedChronoDB owningDB, final BranchInternal owningBranch) {
		super(owningDB, owningBranch);
		this.commitMetadataStore = new ChunkDbCommitMetadataStore(owningDB, owningBranch);
		this.initializeBranch();
		this.initializeKeyspaceToMatrixMapFromDB();
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	protected long getNowInternal() {
		try (LockHolder lock = this.lockNonExclusive()) {
			if (this.cachedNowTimestamp == null) {
				Long storedNowTimestamp = null;
				try (TuplTransaction tx = this.getOwningDB().openTx()) {
					byte[] timestampBinary = tx.load(ChunkedChronoDB.INDEXNAME__BRANCH_TO_NOW,
							this.getOwningBranch().getName());
					storedNowTimestamp = TuplUtils.decodeLong(timestampBinary);
					tx.commit();
				}
				String branchName = this.getOwningBranch().getName();
				BranchChunkManager bcm = this.getOwningDB().getChunkManager().getChunkManagerForBranch(branchName);
				long lastChunkValidFrom = bcm.getChunkForHeadRevision().getMetaData().getValidFrom();
				if (storedNowTimestamp == null) {
					storedNowTimestamp = 0L;
				}
				this.cachedNowTimestamp = Math.max(storedNowTimestamp, lastChunkValidFrom);
			}
			if (this.cachedNowTimestamp == null) {
				return 0L;
			} else {
				return this.cachedNowTimestamp;
			}
		}
	}

	@Override
	protected void setNow(final long timestamp) {
		try (LockHolder lock = this.lockBranchExclusive()) {
			// invalidate cache
			this.cachedNowTimestamp = null;
			try (TuplTransaction tx = this.getOwningDB().openTx()) {
				byte[] timestampBinary = TuplUtils.encodeLong(timestamp);
				tx.store(ChunkedChronoDB.INDEXNAME__BRANCH_TO_NOW, this.getOwningBranch().getName(), timestampBinary);
				tx.commit();
			}
		}
	}

	@Override
	protected TemporalDataMatrix createMatrix(final String keyspace, final long timestamp) {
		String matrixTableName = MatrixUtils.generateRandomName();
		String branchName = this.getOwningBranch().getName();
		try (TuplTransaction tx = this.getOwningDB().openTx()) {
			NavigationIndex.insert(tx, branchName, keyspace, matrixTableName, timestamp);
			TemporalChunkDbDataMatrix matrix = new TemporalChunkDbDataMatrix(this.getOwningDB().getChunkManager(),
					branchName, matrixTableName, keyspace, timestamp);
			tx.commit();
			this.keyspaceToMatrix.put(keyspace, matrix);
			return matrix;
		}
	}

	@Override
	public CommitMetadataStore getCommitMetadataStore() {
		return this.commitMetadataStore;
	}

	@Override
	protected void performWriteAheadLog(final WriteAheadLogToken token) {
		try (TuplTransaction tx = this.getOwningDB().openTx()) {
			byte[] serialForm = this.getOwningDB().getSerializationManager().serialize(token);
			tx.store(ChunkedChronoDB.INDEXNAME__BRANCH_TO_WAL, this.getOwningBranch().getName(), serialForm);
			tx.commit();
		}
	}

	@Override
	protected void clearWriteAheadLogToken() {
		try (TuplTransaction tx = this.getOwningDB().openTx()) {
			tx.delete(ChunkedChronoDB.INDEXNAME__BRANCH_TO_WAL, this.getOwningBranch().getName());
			tx.commit();
		}
	}

	@Override
	protected WriteAheadLogToken getWriteAheadLogTokenIfExists() {
		try (TuplTransaction tx = this.getOwningDB().openTx()) {
			byte[] serialForm = tx.load(ChunkedChronoDB.INDEXNAME__BRANCH_TO_WAL, this.getOwningBranch().getName());
			tx.commit();
			if (serialForm == null) {
				return null;
			} else {
				return (WriteAheadLogToken) this.getOwningDB().getSerializationManager().deserialize(serialForm);
			}
		}
	}

	public void performRollover() {
		long now = this.getNow();
		try (LockHolder lock = this.getOwningDB().lockExclusive()) {
			// record the rollover timestamp
			long timestamp = System.currentTimeMillis();
			if (now == timestamp) {
				// don't rollover exactly at a commit timestamp
				timestamp = now + 1;
			}
			BranchChunkManager chunkManager = this.getOwningDB().getChunkManager()
					.getOrCreateChunkManagerForBranch(this.getOwningBranch());
			// create a file to hold the data. It will be renamed later on.
			File newChunkDataFile = new File(chunkManager.getRootDirectory(),
					"temp." + ChronoChunk.CHUNK_FILE_EXTENSION);
			// make sure that the file is clean (i.e. has no content) and exists
			try {
				if (newChunkDataFile.exists()) {
					boolean deleted = newChunkDataFile.delete();
					if (!deleted) {
						throw new IOException("Failed to delete file '" + newChunkDataFile.getAbsolutePath() + "'!");
					}
				}
				boolean created = newChunkDataFile.createNewFile();
				if (!created) {
					throw new IOException("Failed to create file '" + newChunkDataFile.getAbsolutePath() + "'!");
				}
			} catch (IOException ioe) {
				throw new ChronosIOException("Failed to create data files for rollover!", ioe);
			}
			// fill the new chunk file with the entries from the head revision
			this.transferHeadRevisionIntoChunkDataFile(newChunkDataFile, timestamp);
			// tupl stores it's data in a "<inputFileName>.db" file
			File newChunkDbFile = new File(newChunkDataFile.getAbsolutePath() + "." + TuplUtils.TUPL_DB_FILE_EXTENSION);
			if (newChunkDbFile.exists() == false) {
				throw new IllegalStateException("Failed to create new chunk *.db file!");
			}
			// clear our "now" timestamp cache (creation of new chunk changes timestamp calculation)
			this.cachedNowTimestamp = null;
			// after creating the new chunk, register it at the database and update required metadata
			chunkManager.terminateChunkAndCreateNewHeadRevision(timestamp, newChunkDbFile);
			// make sure that we have an index on the head revision
			this.getOwningDB().getIndexManager().reindexHeadRevision(this.getOwningBranch().getName());

			// purge the entries from the cache that belong to this branch and have open-ended periods,
			// because these periods are now limited to the end of the chunk.
			// NOTE: WE DON'T DO THIS. Read below why.
			// The Mosaic Cache contains e.g. an entry [0;MAX | "hello"->"world"]. If we do a rollover at t=10,
			// then limiting it to the chunk would produce [0;5 | "hello"->"world"]. But an access to
			// key "hello" at timestamp 11 still produces "world", until a write-through occurs - and that will
			// occur in the new chunk. Bottom line is: we do not need to touch the cache here! A rollover is
			// technically a change, but it's a change that doesn't alter the contents of the store!
			// this.getOwningDB().getCache().limitAllOpenEndedPeriodsInBranchTo(this.getOwningBranch().getName(),
			// timestamp);
		}
	}

	private void initializeBranch() {
		try (TuplTransaction tx = this.getOwningDB().openTx()) {
			String branchName = this.getOwningBranch().getName();
			if (NavigationIndex.existsBranch(tx, branchName)) {
				logTrace("Branch '" + branchName + "' already exists.");
				return;
			}
			String keyspaceName = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;
			String tableName = MatrixUtils.generateRandomName();
			logTrace("Creating branch: [" + branchName + ", " + keyspaceName + ", " + tableName + "]");
			NavigationIndex.insert(tx, branchName, keyspaceName, tableName, 0L);
			tx.commit();
		}
	}

	private void initializeKeyspaceToMatrixMapFromDB() {
		try (TuplTransaction tx = this.getOwningDB().openTx()) {
			String branchName = this.getOwningBranch().getName();
			Set<KeyspaceMetadata> allKeyspaceMetadata = NavigationIndex.getKeyspaceMetadata(tx, branchName);
			for (KeyspaceMetadata keyspaceMetadata : allKeyspaceMetadata) {
				String keyspace = keyspaceMetadata.getKeyspaceName();
				String matrixTableName = keyspaceMetadata.getMatrixTableName();
				long timestamp = keyspaceMetadata.getCreationTimestamp();
				TemporalChunkDbDataMatrix matrix = new TemporalChunkDbDataMatrix(this.getOwningDB().getChunkManager(),
						branchName, matrixTableName, keyspace, timestamp);
				this.keyspaceToMatrix.put(keyspace, matrix);
				logTrace("Registering keyspace '" + keyspace + "' matrix in branch '" + branchName + "': "
						+ matrixTableName);
			}
			tx.commit();
		}
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	@Override
	public ChunkedChronoDB getOwningDB() {
		return (ChunkedChronoDB) super.getOwningDB();
	}

	protected static Map<String, Long> getNowTimestampMap(final MapDBTransaction tx) {
		return tx.treeMap(ChunkedChronoDB.INDEXNAME__BRANCH_TO_NOW, Serializer.STRING, Serializer.LONG);
	}

	protected static Map<String, byte[]> getWalTokenMap(final MapDBTransaction tx) {
		return tx.treeMap(ChunkedChronoDB.INDEXNAME__BRANCH_TO_WAL, Serializer.STRING, Serializer.BYTE_ARRAY);
	}

	private void transferHeadRevisionIntoChunkDataFile(final File newChunkDataFile, final long timestamp) {
		String branchName = this.getOwningBranch().getName();
		GlobalChunkManager chunkManager = this.getOwningDB().getChunkManager();
		SerializationManager serializationManager = this.getOwningDB().getSerializationManager();
		Set<KeyspaceMetadata> keyspaceMetadata = null;
		try (TuplTransaction rootDbTx = this.getOwningDB().openTx()) {
			keyspaceMetadata = NavigationIndex.getKeyspaceMetadata(rootDbTx, branchName);
		}
		Map<String, String> keyspaceNameToMapName = Maps.newHashMap();
		for (KeyspaceMetadata metadata : keyspaceMetadata) {
			String keyspaceName = metadata.getKeyspaceName();
			String matrixTableName = metadata.getMatrixTableName();
			keyspaceNameToMapName.put(keyspaceName, matrixTableName);
		}
		ChronoDBTransaction tx = this.getOwningDB().tx(branchName);
		Set<UnqualifiedTemporalEntry> entries = Sets.newHashSet();
		int maxBatchSize = TuplUtils.BATCH_INSERT_THRESHOLD;
		for (String keyspace : tx.keyspaces()) {
			String mapName = keyspaceNameToMapName.get(keyspace);
			Set<String> keySet = tx.keySet(keyspace);
			for (String key : keySet) {
				Object value = tx.get(keyspace, key);
				if (value == null) {
					// this should actually never happen because removed keys do not
					// appear in the keyset anymore. This is just to be safe.
					continue;
				}
				byte[] serializedValue = serializationManager.serialize(value);
				UnqualifiedTemporalKey utKey = new UnqualifiedTemporalKey(key, timestamp);
				UnqualifiedTemporalEntry utEntry = new UnqualifiedTemporalEntry(utKey, serializedValue);
				entries.add(utEntry);
				if (entries.size() >= maxBatchSize) {
					// flush the data onto disk
					try (TuplTransaction tuplTx = chunkManager.openTransactionOn(newChunkDataFile)) {
						TuplDataMatrixUtil.insertEntriesBatch(tuplTx, mapName, keyspace, entries);
						entries.clear();
						tuplTx.commit();
					}
				}
			}
			if (entries.isEmpty() == false) {
				// perform a last flush
				try (TuplTransaction tuplTx = chunkManager.openTransactionOn(newChunkDataFile)) {
					TuplDataMatrixUtil.insertEntriesBatch(tuplTx, mapName, keyspace, entries);
					entries.clear();
					tuplTx.commit();
				}
			}
		}
		// make sure that the MapDB instance is closed
		chunkManager.ensureTuplDbIsClosed(newChunkDataFile);
	}

}
