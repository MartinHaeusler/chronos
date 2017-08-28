package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.DumpOption;
import org.chronos.chronodb.api.builder.transaction.ChronoDBTransactionBuilder;
import org.chronos.chronodb.api.dump.ChronoDBDumpFormat;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.api.exceptions.ChronosBuildVersionConflictException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionBranchException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionTimestampException;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.CommitMetadataStore;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.api.TransactionConfigurationInternal;
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.api.stream.ObjectInput;
import org.chronos.chronodb.internal.api.stream.ObjectOutput;
import org.chronos.chronodb.internal.impl.builder.transaction.DefaultTransactionBuilder;
import org.chronos.chronodb.internal.impl.dump.ChronoDBDumpUtil;
import org.chronos.chronodb.internal.impl.dump.CommitMetadataMap;
import org.chronos.chronodb.internal.impl.dump.DumpOptions;
import org.chronos.chronodb.internal.util.ThreadBound;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.version.ChronosVersion;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public abstract class AbstractChronoDB implements ChronoDB, ChronoDBInternal {

	protected final ReadWriteLock dbLock;

	private final ChronoDBConfiguration configuration;
	private final Set<ChronoDBShutdownHook> shutdownHooks;

	private final ThreadBound<LockHolder> exclusiveLockHolder;
	private final ThreadBound<LockHolder> nonExclusiveLockHolder;

	private boolean closed = false;

	protected AbstractChronoDB(final ChronoDBConfiguration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		this.configuration = configuration;
		this.dbLock = new ReentrantReadWriteLock(false);
		this.exclusiveLockHolder = ThreadBound.createWeakReference();
		this.nonExclusiveLockHolder = ThreadBound.createWeakReference();
		this.shutdownHooks = Collections.synchronizedSet(Sets.newHashSet());
	}

	// =================================================================================================================
	// POST CONSTRUCT
	// =================================================================================================================

	@Override
	public void postConstruct() {
		// check the chronos build version
		this.updateBuildVersionInDatabase();
		// check if any branch needs recovery
		for (Branch branch : this.getBranchManager().getBranches()) {
			TemporalKeyValueStore tkvs = ((BranchInternal) branch).getTemporalKeyValueStore();
			tkvs.performStartupRecoveryIfRequired();
		}
	}

	// =================================================================================================================
	// SHUTDOWN HANDLING
	// =================================================================================================================

	public void addShutdownHook(final ChronoDBShutdownHook hook) {
		checkNotNull(hook, "Precondition violation - argument 'hook' must not be NULL!");
		try (LockHolder lock = this.lockNonExclusive()) {
			this.shutdownHooks.add(hook);
		}
	}

	public void removeShutdownHook(final ChronoDBShutdownHook hook) {
		checkNotNull(hook, "Precondition violation - argument 'hook' must not be NULL!");
		try (LockHolder lock = this.lockNonExclusive()) {
			this.shutdownHooks.remove(hook);
		}
	}

	@Override
	public final void close() {
		if (this.isClosed()) {
			return;
		}
		try (LockHolder lock = this.lockExclusive()) {
			for (ChronoDBShutdownHook hook : this.shutdownHooks) {
				hook.onShutdown();
			}
			this.closed = true;
		}
	}

	@Override
	public final boolean isClosed() {
		return this.closed;
	}

	// =================================================================================================================
	// TRANSACTION HANDLING
	// =================================================================================================================

	@Override
	public ChronoDBTransactionBuilder txBuilder() {
		return new DefaultTransactionBuilder(this);
	}

	@Override
	public ChronoDBTransaction tx(final TransactionConfigurationInternal configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		try (LockHolder lock = this.lockNonExclusive()) {
			String branchName = configuration.getBranch();
			if (this.getBranchManager().existsBranch(branchName) == false) {
				throw new InvalidTransactionBranchException(
						"There is no branch '" + branchName + "' in this ChronoDB!");
			}
			TemporalKeyValueStore tkvs = this.getTKVS(branchName);
			long now = tkvs.getNow();
			if (configuration.isTimestampNow() == false && configuration.getTimestamp() > now) {
				ChronoLogger.logDebug("Invalid timestamp. Requested = " + configuration.getTimestamp() + ", now = "
						+ now + ", branch = '" + branchName + "'");
				throw new InvalidTransactionTimestampException(
						"Cannot open transaction at the given date or timestamp: it's after the latest commit! Latest commit: "
								+ now + ", transaction timestamp: " + configuration.getTimestamp());
			}
			return tkvs.tx(configuration);
		}
	}

	// =================================================================================================================
	// DUMP CREATION & DUMP LOADING
	// =================================================================================================================

	@Override
	public CloseableIterator<ChronoDBEntry> entryStream() {
		Set<String> branchNames = this.getBranchManager().getBranchNames();
		Iterator<String> branchIterator = branchNames.iterator();
		long timestamp = System.currentTimeMillis();
		Iterator<CloseableIterator<ChronoDBEntry>> branchStreams = Iterators.transform(branchIterator,
				(final String branch) -> {
					TemporalKeyValueStore tkvs = this.getTKVS(branch);
					return tkvs.allEntriesIterator(timestamp);
				});
		return CloseableIterator.concat(branchStreams);
	}

	@Override
	public void loadEntries(final List<ChronoDBEntry> entries) {
		checkNotNull(entries, "Precondition violation - argument 'entries' must not be NULL!");
		SetMultimap<String, ChronoDBEntry> branchToEntries = HashMultimap.create();
		for (ChronoDBEntry entry : entries) {
			String branchName = entry.getIdentifier().getBranchName();
			branchToEntries.put(branchName, entry);
		}
		// insert into the branches
		for (String branchName : branchToEntries.keySet()) {
			Set<ChronoDBEntry> branchEntries = branchToEntries.get(branchName);
			BranchInternal branch = (BranchInternal) this.getBranchManager().getBranch(branchName);
			branch.getTemporalKeyValueStore().insertEntries(branchEntries);
		}
	}

	@Override
	public void loadCommitTimestamps(final CommitMetadataMap commitMetadata) {
		checkNotNull(commitMetadata, "Precondition violation - argument 'commitMetadata' must not be NULL!");
		for (String branchName : commitMetadata.getContainedBranches()) {
			SortedMap<Long, Object> branchCommitMetadata = commitMetadata.getCommitMetadataForBranch(branchName);
			BranchInternal branch = (BranchInternal) this.getBranchManager().getBranch(branchName);
			CommitMetadataStore metadataStore = branch.getTemporalKeyValueStore().getCommitMetadataStore();
			for (Entry<Long, Object> entry : branchCommitMetadata.entrySet()) {
				metadataStore.put(entry.getKey(), entry.getValue());
			}
		}
	}

	@Override
	public void writeDump(final File dumpFile, final DumpOption... dumpOptions) {
		checkNotNull(dumpFile, "Precondition violation - argument 'dumpFile' must not be NULL!");
		if (dumpFile.exists()) {
			checkArgument(dumpFile.isFile(),
					"Precondition violation - argument 'dumpFile' must be a file (not a directory)!");
		} else {
			try {
				dumpFile.getParentFile().mkdirs();
				dumpFile.createNewFile();
			} catch (IOException e) {
				throw new ChronoDBStorageBackendException(
						"Failed to create dump file in '" + dumpFile.getAbsolutePath() + "'!", e);
			}
		}
		DumpOptions options = new DumpOptions(dumpOptions);
		try (LockHolder lock = this.lockNonExclusive()) {
			try (ObjectOutput output = ChronoDBDumpFormat.createOutput(dumpFile, options)) {
				ChronoDBDumpUtil.dumpDBContentsToOutput(this, output, options);
			}
		}
	}

	@Override
	public void readDump(final File dumpFile, final DumpOption... dumpOptions) {
		checkNotNull(dumpFile, "Precondition violation - argument 'dumpFile' must not be NULL!");
		checkArgument(dumpFile.exists(),
				"Precondition violation - argument 'dumpFile' does not exist! Location: " + dumpFile.getAbsolutePath());
		checkArgument(dumpFile.isFile(),
				"Precondition violation - argument 'dumpFile' must be a File (is a Directory)!");
		DumpOptions options = new DumpOptions(dumpOptions);
		try (LockHolder lock = this.lockExclusive()) {
			try (ObjectInput input = ChronoDBDumpFormat.createInput(dumpFile, options)) {
				ChronoDBDumpUtil.readDumpContentsFromInput(this, input, options);
			}
		}
	}

	// =================================================================================================================
	// LOCKING
	// =================================================================================================================

	@Override
	public LockHolder lockExclusive() {
		LockHolder lockHolder = this.exclusiveLockHolder.get();
		if (lockHolder == null) {
			lockHolder = LockHolder.createBasicLockHolderFor(this.dbLock.writeLock());
			this.exclusiveLockHolder.set(lockHolder);
		}
		// lockHolder.releaseLock() is called on lockHolder.close()
		lockHolder.acquireLock();
		return lockHolder;
	}

	@Override
	public LockHolder lockNonExclusive() {
		LockHolder lockHolder = this.nonExclusiveLockHolder.get();
		if (lockHolder == null) {
			lockHolder = LockHolder.createBasicLockHolderFor(this.dbLock.readLock());
			this.nonExclusiveLockHolder.set(lockHolder);
		}
		// lockHolder.releaseLock() is called on lockHolder.close()
		lockHolder.acquireLock();
		return lockHolder;
	}

	// =================================================================================================================
	// MISCELLANEOUS
	// =================================================================================================================

	@Override
	public ChronoDBConfiguration getConfiguration() {
		return this.configuration;
	}

	protected TemporalKeyValueStore getTKVS(final String branchName) {
		BranchInternal branch = (BranchInternal) this.getBranchManager().getBranch(branchName);
		return branch.getTemporalKeyValueStore();
	}

	// =====================================================================================================================
	// ABSTRACT METHOD DECLARATIONS
	// =====================================================================================================================

	/**
	 * Updates the Chronos Build Version in the database.
	 *
	 * <p>
	 * Implementations of this method should follow this algorithm:
	 * <ol>
	 * <li>Check if the version identifier in the database exists.
	 * <ol>
	 * <li>If there is no version identifier, write {@link ChronosVersion#getCurrentVersion()}.
	 * <li>If there is a version identifier, and it is smaller than {@link ChronosVersion#getCurrentVersion()}, overwrite it (<i>Note:</i> in the future, we may perform data migration steps here).
	 * <li>If there is a version identifier larger than {@link ChronosVersion#getCurrentVersion()}, throw a {@link ChronosBuildVersionConflictException}.
	 * </ol>
	 * </ol>
	 */
	protected abstract void updateBuildVersionInDatabase();

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	public interface ChronoDBShutdownHook {

		public void onShutdown();

	}

}
