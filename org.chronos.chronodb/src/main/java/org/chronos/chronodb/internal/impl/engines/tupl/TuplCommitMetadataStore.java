package org.chronos.chronodb.internal.impl.engines.tupl;

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
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.cojen.tupl.Cursor;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TuplCommitMetadataStore extends AbstractCommitMetadataStore {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	private static final String INDEX_SUFFIX = "_CommitMetadata";

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	private final String indexName;

	protected TuplCommitMetadataStore(final TuplChronoDB owningDB, final Branch owningBranch) {
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
		try (DefaultTuplTransaction tx = this.openTransaction()) {
			byte[] key = TuplUtils.encodeLong(commitTimestamp);
			tx.store(this.indexName, key, metadata);
			tx.commit();
		}
	}

	@Override
	protected byte[] getInternal(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		try (DefaultTuplTransaction tx = this.openTransaction()) {
			byte[] key = TuplUtils.encodeLong(timestamp);
			return tx.load(this.indexName, key);
		}
	}

	@Override
	protected void rollbackToTimestampInternal(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		try (DefaultTuplTransaction tx = this.openTransaction()) {
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
				throw new ChronoDBStorageBackendException(
						"Failed to roll back commit metadata! See root cause for details.", ioe);
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
	public List<Entry<Long, Object>> getCommitMetadataAround(final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		List<Entry<Long, Object>> resultList = Lists.newArrayList();
		// shortcut if count is zero...
		if (count == 0) {
			return resultList;
		}
		// open a cursor
		try (TuplTransaction tx = this.openTransaction()) {
			Cursor cursor = tx.newCursorOn(this.indexName);
			try {
				// we perform loading manually in this algorithm
				cursor.autoload(false);
				// position the cursor at the requested timestamp
				cursor.findGe(TuplUtils.encodeLong(timestamp));
				if (cursor.key() == null) {
					// we found no matching greater/equal key...
					cursor.findLe(TuplUtils.encodeLong(timestamp));
					if (cursor.key() == null) {
						// we found no matching less/equal key. The index is empty!
						return resultList;
					}
					// there are no greater timestamps, return the lower ones
					int added = 0;
					while (added < count) {
						resultList.add(this.entryAt(cursor));
						added++;
						cursor.previous();
						if (cursor.key() == null) {
							// reached end of the index
							break;
						}
					}
					resultList.sort(EntryTimestampComparator.INSTANCE.reversed());
					return resultList;
				}
				// we have found our "middle" key
				resultList.add(this.entryAt(cursor));
				SearchMetadataAroundTimestampState state = new SearchMetadataAroundTimestampState(cursor.key());

				// add the uppper half
				int limitUpper = (count - 1) / 2;
				this.addEntriesAbove(resultList, cursor, state, limitUpper);
				boolean aboveExhausted = state.getAddedUpper() < limitUpper;
				// take the remaining stuff from the lower bound
				int limitLower = count - state.getAddedUpper() - 1;
				this.addEntriesBelow(resultList, cursor, state, limitLower);
				boolean belowExhausted = state.getAddedLower() < limitLower;
				if (resultList.size() < count && (aboveExhausted && belowExhausted) == false) {
					if (aboveExhausted == false && belowExhausted) {
						// lower end is exhausted, add the rest from the upper end
						int toAddUpper = count - 1 - state.getAddedUpper() - state.getAddedLower();
						this.addEntriesAbove(resultList, cursor, state, toAddUpper);
					} else if (aboveExhausted && belowExhausted == false) {
						// upper end is exhausted, add the rest from the lower end
						int toAddLower = count - 1 - state.getAddedUpper() - state.getAddedLower();
						this.addEntriesBelow(resultList, cursor, state, toAddLower);
					} else {
						// this can't happen. It would mean that we still have elements to add
						// on both sides, but we did not achieve our requested list length...
						throw new RuntimeException("Unreachable code block was reached!");
					}
				}
				resultList.sort(EntryTimestampComparator.INSTANCE.reversed());
				return resultList;
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

	private void addEntriesAbove(final List<Entry<Long, Object>> resultList, final Cursor cursor, final SearchMetadataAroundTimestampState state, final int toAdd) throws IOException {
		int added = 0;
		cursor.findNearby(state.getHighestKey());
		while (added < toAdd) {
			cursor.next();
			if (cursor.key() == null) {
				// no more entries in this direction
				break;
			}
			resultList.add(this.entryAt(cursor));
			state.setHighestKey(cursor.key());
			added++;
			state.setAddedUpper(added);
		}
	}

	private void addEntriesBelow(final List<Entry<Long, Object>> resultList, final Cursor cursor, final SearchMetadataAroundTimestampState state, final int toAdd) throws IOException {
		int added = 0;
		cursor.findNearby(state.getLowestKey());
		while (added < toAdd) {
			cursor.previous();
			if (cursor.key() == null) {
				// no more entries in this direction
				break;
			}
			resultList.add(this.entryAt(cursor));
			state.setLowestKey(cursor.key());
			added++;
			state.setAddedLower(added);
		}
	}

	@Override
	public List<Entry<Long, Object>> getCommitMetadataBefore(final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		List<Entry<Long, Object>> resultList = Lists.newArrayList();
		// shortcut if count is zero...
		if (count == 0) {
			return resultList;
		}
		// open a cursor
		try (TuplTransaction tx = this.openTransaction()) {
			Cursor cursor = tx.newCursorOn(this.indexName);
			try {
				// we perform loading manually in this algorithm
				cursor.autoload(false);
				// find the next-lower key for the given timestamp
				cursor.findLt(TuplUtils.encodeLong(timestamp));
				if (cursor.key() == null) {
					// there are no commits strictly before the given timestamp
					return resultList;
				}
				// get the entries by linear iteration
				while (resultList.size() < count) {
					resultList.add(this.entryAt(cursor));
					cursor.previous();
					if (cursor.key() == null) {
						// there are no more commits in this direction
						break;
					}
				}
				resultList.sort(EntryTimestampComparator.INSTANCE.reversed());
				return resultList;
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
	public List<Entry<Long, Object>> getCommitMetadataAfter(final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		List<Entry<Long, Object>> resultList = Lists.newArrayList();
		// shortcut if count is zero...
		if (count == 0) {
			return resultList;
		}
		// open a cursor
		try (TuplTransaction tx = this.openTransaction()) {
			Cursor cursor = tx.newCursorOn(this.indexName);
			try {
				// we perform loading manually in this algorithm
				cursor.autoload(false);
				// find the next-larger key for the given timestamp
				cursor.findGt(TuplUtils.encodeLong(timestamp));
				if (cursor.key() == null) {
					// there are no commits strictly before the given timestamp
					return resultList;
				}
				// get the entries by linear iteration
				while (resultList.size() < count) {
					resultList.add(this.entryAt(cursor));
					cursor.next();
					if (cursor.key() == null) {
						// there are no more commits in this direction
						break;
					}
				}
				resultList.sort(EntryTimestampComparator.INSTANCE.reversed());
				return resultList;
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
	protected TuplChronoDB getOwningDB() {
		return (TuplChronoDB) super.getOwningDB();
	}

	private DefaultTuplTransaction openTransaction() {
		return this.getOwningDB().openTransaction();
	}

	private Entry<Long, Object> entryAt(final Cursor cursor) throws IOException {
		if (cursor.autoload() == false) {
			cursor.load();
		}
		long timestamp = TuplUtils.decodeLong(cursor.key());
		byte[] data = cursor.value();
		Object commitMetadata = this.deserialize(data);
		return Pair.of(timestamp, commitMetadata);
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

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private static class SearchMetadataAroundTimestampState {

		private byte[] lowestKey;
		private byte[] highestKey;

		private int addedLower;
		private int addedUpper;

		public SearchMetadataAroundTimestampState(final byte[] middleKey) {
			checkNotNull(middleKey, "Precondition violation - argument 'middleKey' must not be NULL!");
			this.lowestKey = middleKey;
			this.highestKey = middleKey;
		}

		public byte[] getLowestKey() {
			return this.lowestKey;
		}

		public void setLowestKey(final byte[] lowestKey) {
			this.lowestKey = lowestKey;
		}

		public byte[] getHighestKey() {
			return this.highestKey;
		}

		public void setHighestKey(final byte[] highestKey) {
			this.highestKey = highestKey;
		}

		public int getAddedLower() {
			return this.addedLower;
		}

		public void setAddedLower(final int addedLower) {
			this.addedLower = addedLower;
		}

		public int getAddedUpper() {
			return this.addedUpper;
		}

		public void setAddedUpper(final int addedUpper) {
			this.addedUpper = addedUpper;
		}

	}

}
