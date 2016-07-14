package org.chronos.chronodb.internal.impl.engines.inmemory;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.atomic.AtomicLong;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.CommitMetadataStore;
import org.chronos.chronodb.internal.api.TemporalDataMatrix;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalKeyValueStore;
import org.chronos.chronodb.internal.impl.engines.base.WriteAheadLogToken;

public class InMemoryTKVS extends AbstractTemporalKeyValueStore implements TemporalKeyValueStore {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final AtomicLong now = new AtomicLong(0);
	private final CommitMetadataStore commitMetadataStore;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public InMemoryTKVS(final ChronoDBInternal db, final BranchInternal branch) {
		super(db, branch);
		this.commitMetadataStore = new InMemoryCommitMetadataStore(db, branch);
		TemporalDataMatrix matrix = this.createMatrix(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, 0L);
		this.keyspaceToMatrix.put(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, matrix);
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	@Override
	protected long getNowInternal() {
		return this.now.get();
	}

	@Override
	protected void setNow(final long timestamp) {
		this.now.set(timestamp);
	}

	@Override
	protected TemporalDataMatrix createMatrix(final String keyspace, final long timestamp) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		return new TemporalInMemoryMatrix(keyspace, timestamp);
	}

	@Override
	protected void performWriteAheadLog(final WriteAheadLogToken token) {
		// in-memory backend does not support Write-Ahead-Logging.
		// This call is ignored.
	}

	@Override
	protected void clearWriteAheadLogToken() {
		// in-memory backend does not support Write-Ahead-Logging.
		// This call is ignored.
	}

	@Override
	public void performStartupRecoveryIfRequired() {
		// startup recovery is never needed for in-memory elements
	}

	@Override
	protected WriteAheadLogToken getWriteAheadLogTokenIfExists() {
		// in-memory backend does not store Write-Ahead-Log Tokens.
		return null;
	}

	@Override
	protected CommitMetadataStore getCommitMetadataStore() {
		return this.commitMetadataStore;
	}

}
