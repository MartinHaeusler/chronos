package org.chronos.chronodb.internal.impl.engines.chunkdb;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.internal.impl.engines.base.AbstractCommitMetadataStore;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplUtils;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.chronos.common.exceptions.ChronosIOException;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.cojen.tupl.Cursor;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ChunkDbCommitMetadataStore extends AbstractCommitMetadataStore {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	private static final String INDEX_SUFFIX = "_CommitMetadata";

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	private final String indexName;

	protected ChunkDbCommitMetadataStore(final ChunkedChronoDB owningDB, final Branch owningBranch) {
		super(owningDB, owningBranch);
		this.indexName = this.getBranchName() + INDEX_SUFFIX;
	}

	// =====================================================================================================================
	// API IMPLEMENTATION
	// =====================================================================================================================

	@Override
	protected void putInternal(final long commitTimestamp, final byte[] metadata) {
		checkArgument(commitTimestamp >= 0,
				"Precondition violation - argument 'commitTimestamp' must not be negative!");
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		try (TuplTransaction tx = this.openTransaction()) {
			byte[] key = TuplUtils.encodeLong(commitTimestamp);
			tx.store(this.indexName, key, metadata);
			tx.commit();
		}
	}

	@Override
	protected byte[] getInternal(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		try (TuplTransaction tx = this.openTransaction()) {
			byte[] key = TuplUtils.encodeLong(timestamp);
			return tx.load(this.indexName, key);
		}
	}

	@Override
	protected void rollbackToTimestampInternal(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		try (TuplTransaction tx = this.openTransaction()) {
			Set<byte[]> keysToDelete = Sets.newHashSet();
			Cursor cursor = tx.newCursorOn(this.indexName);
			try {
				// disable automatic loading of values; we are only interested in keys here
				cursor.autoload(false);
				cursor.first();
				while (cursor.key() != null) {
					byte[] key = cursor.key();
					Long longKey = TuplUtils.decodeLong(key);
					if (longKey > timestamp) {
						keysToDelete.add(key);
					}
					cursor.next();
				}
			} catch (IOException ioe) {
				throw new ChronosIOException("Failed to roll back commit metadata! See root cause for details.", ioe);
			} finally {
				if (cursor != null) {
					cursor.reset();
				}
			}
			for (byte[] key : keysToDelete) {
				tx.delete(this.indexName, key);
			}
			tx.commit();
		}
	}

	@Override
	public Iterator<Long> getCommitTimestampsBetween(final long from, final long to, final Order order) {
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		// fail-fast if the period is empty
		if (from > to) {
			return Collections.emptyIterator();
		}
		try (TuplTransaction tx = this.openTransaction()) {
			Cursor cursor = tx.newCursorOn(this.indexName);
			try {
				// disable automatic loading of values; we are only interested in keys here
				cursor.autoload(false);
				initializeCursorPosition(cursor, order);
				if (cursor.key() == null) {
					// commit metadata table is empty
					return Collections.emptyIterator();
				}
				switch (order) {
				case ASCENDING:
					cursor.findGe(TuplUtils.encodeLong(from));
					break;
				case DESCENDING:
					cursor.findLe(TuplUtils.encodeLong(to));
					break;
				default:
					throw new UnknownEnumLiteralException(order);
				}
				List<Long> timestamps = Lists.newArrayList();
				while (cursor.key() != null) {
					long timestamp = TuplUtils.decodeLong(cursor.key());
					if (timestamp < from || timestamp > to) {
						break;
					}
					timestamps.add(timestamp);
					moveCursor(cursor, order);
				}
				return Iterators.unmodifiableIterator(timestamps.iterator());
			} catch (IOException e) {
				throw new ChronoDBStorageBackendException(
						"Failed to access commit metadata! See root cause for details.", e);
			} finally {
				if (cursor != null) {
					cursor.reset();
				}
			}
		}
	}

	@Override
	public Iterator<Entry<Long, Object>> getCommitMetadataBetween(final long from, final long to, final Order order) {
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		// fail-fast if the period is empty
		if (from > to) {
			return Collections.emptyIterator();
		}
		try (TuplTransaction tx = this.openTransaction()) {
			Cursor cursor = tx.newCursorOn(this.indexName);
			try {
				cursor.autoload(true);
				initializeCursorPosition(cursor, order);
				if (cursor.key() == null) {
					// commit metadata table is empty
					return Collections.emptyIterator();
				}
				switch (order) {
				case ASCENDING:
					cursor.findGe(TuplUtils.encodeLong(from));
					break;
				case DESCENDING:
					cursor.findLe(TuplUtils.encodeLong(to));
					break;
				default:
					throw new UnknownEnumLiteralException(order);
				}
				List<Entry<Long, Object>> timestamps = Lists.newArrayList();
				while (cursor.key() != null) {
					long timestamp = TuplUtils.decodeLong(cursor.key());
					if (timestamp < from || timestamp > to) {
						break;
					}
					byte[] metadataBytes = cursor.value();
					Object metadata = this.deserialize(metadataBytes);
					timestamps.add(Pair.of(timestamp, metadata));
					moveCursor(cursor, order);
				}
				return Iterators.unmodifiableIterator(timestamps.iterator());
			} catch (IOException e) {
				throw new ChronoDBStorageBackendException(
						"Failed to access commit metadata! See root cause for details.", e);
			} finally {
				if (cursor != null) {
					cursor.reset();
				}
			}
		}
	}

	@Override
	public Iterator<Long> getCommitTimestampsPaged(final long minTimestamp, final long maxTimestamp, final int pageSize,
			final int pageIndex, final Order order) {
		checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
		checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
		checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
		checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		// fail-fast if the period is empty
		if (minTimestamp > maxTimestamp) {
			return Collections.emptyIterator();
		}
		try (TuplTransaction tx = this.openTransaction()) {
			Cursor cursor = tx.newCursorOn(this.indexName);
			try {
				cursor.autoload(false);
				initializeCursorPosition(cursor, order);
				if (cursor.key() == null) {
					// commit metadata table is empty
					return Collections.emptyIterator();
				}
				switch (order) {
				case ASCENDING:
					cursor.findGe(TuplUtils.encodeLong(minTimestamp));
					break;
				case DESCENDING:
					cursor.findLe(TuplUtils.encodeLong(maxTimestamp));
					break;
				default:
					throw new UnknownEnumLiteralException(order);
				}
				// move the cursor until we arrive at the desired page
				for (int i = 0; i < pageSize * pageIndex && cursor.key() != null; i++) {
					moveCursor(cursor, order);
				}
				// fill the page
				List<Long> timestamps = Lists.newArrayList();
				while (cursor.key() != null && timestamps.size() < pageSize) {
					long timestamp = TuplUtils.decodeLong(cursor.key());
					if (timestamp < minTimestamp || timestamp > maxTimestamp) {
						break;
					}
					timestamps.add(timestamp);
					moveCursor(cursor, order);
				}
				return Iterators.unmodifiableIterator(timestamps.iterator());

			} catch (IOException e) {
				throw new ChronoDBStorageBackendException(
						"Failed to access commit metadata! See root cause for details.", e);
			} finally {
				if (cursor != null) {
					cursor.reset();
				}
			}
		}

	}

	@Override
	public Iterator<Entry<Long, Object>> getCommitMetadataPaged(final long minTimestamp, final long maxTimestamp,
			final int pageSize, final int pageIndex, final Order order) {
		checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
		checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
		checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
		checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		// fail-fast if the period is empty
		if (minTimestamp > maxTimestamp) {
			return Collections.emptyIterator();
		}
		try (TuplTransaction tx = this.openTransaction()) {
			Cursor cursor = tx.newCursorOn(this.indexName);
			try {
				cursor.autoload(true);
				initializeCursorPosition(cursor, order);
				if (cursor.key() == null) {
					// commit metadata table is empty
					return Collections.emptyIterator();
				}
				switch (order) {
				case ASCENDING:
					cursor.findGe(TuplUtils.encodeLong(minTimestamp));
					break;
				case DESCENDING:
					cursor.findLe(TuplUtils.encodeLong(maxTimestamp));
					break;
				default:
					throw new UnknownEnumLiteralException(order);
				}
				// move the cursor until we arrive at the desired page
				for (int i = 0; i < pageSize * pageIndex && cursor.key() != null; i++) {
					moveCursor(cursor, order);
				}
				// fill the page
				List<Entry<Long, Object>> metadataList = Lists.newArrayList();
				while (cursor.key() != null && metadataList.size() < pageSize) {
					long timestamp = TuplUtils.decodeLong(cursor.key());
					if (timestamp < minTimestamp || timestamp > maxTimestamp) {
						break;
					}
					byte[] metadataBytes = cursor.value();
					Object metadata = this.deserialize(metadataBytes);
					metadataList.add(Pair.of(timestamp, metadata));
					moveCursor(cursor, order);
				}
				return Iterators.unmodifiableIterator(metadataList.iterator());
			} catch (IOException e) {
				throw new ChronoDBStorageBackendException(
						"Failed to access commit metadata! See root cause for details.", e);
			} finally {
				if (cursor != null) {
					cursor.reset();
				}
			}
		}
	}

	@Override
	public int countCommitTimestampsBetween(final long from, final long to) {
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		// fail-fast if the period is empty
		if (from > to) {
			return 0;
		}
		// note: I believe that there is no faster way than doing it like this. Maybe it could
		// be optimized, but it should at least work this way.
		return Iterators.size(this.getCommitTimestampsBetween(from, to));
	}

	@Override
	public int countCommitTimestamps() {
		try (TuplTransaction tx = this.openTransaction()) {
			Cursor cursor = tx.newCursorOn(this.indexName);
			try {
				cursor.autoload(false);
				cursor.first();
				int size = 0;
				while (cursor.key() != null) {
					size++;
					cursor.next();
				}
				return size;
			} catch (IOException e) {
				throw new ChronoDBStorageBackendException(
						"Failed to access commit metadata! See root cause for details.", e);
			} finally {
				if (cursor != null) {
					cursor.reset();
				}
			}
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	@Override
	protected ChunkedChronoDB getOwningDB() {
		return (ChunkedChronoDB) super.getOwningDB();
	}

	private TuplTransaction openTransaction() {
		return this.getOwningDB().openTx();
	}

	private static void initializeCursorPosition(final Cursor cursor, final Order order) throws IOException {
		switch (order) {
		case ASCENDING:
			cursor.first();
			break;
		case DESCENDING:
			cursor.last();
			break;
		default:
			throw new UnknownEnumLiteralException(order);
		}
	}

	private static void moveCursor(final Cursor cursor, final Order order) throws IOException {
		switch (order) {
		case ASCENDING:
			cursor.next();
			break;
		case DESCENDING:
			cursor.previous();
			break;
		default:
			throw new UnknownEnumLiteralException(order);
		}
	}
}
