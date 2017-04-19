package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.internal.impl.engines.base.AbstractCommitMetadataStore;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

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

	@Override
	public Iterator<Long> getCommitTimestampsBetween(final long from, final long to, final Order order) {
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		String branchName = this.getBranchName();
		try (Connection connection = this.openConnection()) {
			return JdbcCommitMetadataTable.get(connection).getCommitTimestampsBetween(branchName, from, to, order);
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to access Commit Metadata Table!", e);
		}
	}

	@Override
	public Iterator<Entry<Long, Object>> getCommitMetadataBetween(final long from, final long to, final Order order) {
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		String branchName = this.getBranchName();
		try (Connection connection = this.openConnection()) {
			Iterator<Entry<Long, byte[]>> iterator = JdbcCommitMetadataTable.get(connection)
					.getCommitMetadataBetween(branchName, from, to, order);
			return Iterators.transform(iterator, pair -> this.mapSerialEntryToPair(pair));
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to access Commit Metadata Table!", e);
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
		String branchName = this.getBranchName();
		try (Connection connection = this.openConnection()) {
			return JdbcCommitMetadataTable.get(connection).getCommitTimestampsPaged(branchName, minTimestamp,
					maxTimestamp, pageSize, pageIndex, order);
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to access Commit Metadata Table!", e);
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
		String branchName = this.getBranchName();
		try (Connection connection = this.openConnection()) {
			Iterator<Entry<Long, byte[]>> iterator = JdbcCommitMetadataTable.get(connection)
					.getCommitMetadataPaged(branchName, minTimestamp, maxTimestamp, pageSize, pageIndex, order);
			return Iterators.transform(iterator, pair -> this.mapSerialEntryToPair(pair));
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to access Commit Metadata Table!", e);
		}
	}

	@Override
	public List<Entry<Long, Object>> getCommitMetadataAround(final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		String branchName = this.getBranchName();
		try (Connection connection = this.openConnection()) {
			List<Entry<Long, byte[]>> list = JdbcCommitMetadataTable.get(connection).getCommitMetadataAround(branchName, timestamp, count);
			List<Entry<Long, Object>> deserializedList = Lists.newArrayList();
			list.forEach(e -> deserializedList.add(this.deserializeValueOf(e)));
			deserializedList.sort(EntryTimestampComparator.INSTANCE.reversed());
			return deserializedList;
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to access Commit Metadata Table!", e);
		}
	}

	@Override
	public List<Entry<Long, Object>> getCommitMetadataBefore(final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		String branchName = this.getBranchName();
		try (Connection connection = this.openConnection()) {
			List<Entry<Long, byte[]>> list = JdbcCommitMetadataTable.get(connection).getCommitMetadataBefore(branchName, timestamp, count);
			List<Entry<Long, Object>> deserializedList = Lists.newArrayList();
			list.forEach(e -> deserializedList.add(this.deserializeValueOf(e)));
			deserializedList.sort(EntryTimestampComparator.INSTANCE.reversed());
			return deserializedList;
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to access Commit Metadata Table!", e);
		}
	}

	@Override
	public List<Entry<Long, Object>> getCommitMetadataAfter(final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		String branchName = this.getBranchName();
		try (Connection connection = this.openConnection()) {
			List<Entry<Long, byte[]>> list = JdbcCommitMetadataTable.get(connection).getCommitMetadataAfter(branchName, timestamp, count);
			List<Entry<Long, Object>> deserializedList = Lists.newArrayList();
			list.forEach(e -> deserializedList.add(this.deserializeValueOf(e)));
			deserializedList.sort(EntryTimestampComparator.INSTANCE.reversed());
			return deserializedList;
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to access Commit Metadata Table!", e);
		}
	}

	@Override
	public int countCommitTimestampsBetween(final long from, final long to) {
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		String branchName = this.getBranchName();
		try (Connection connection = this.openConnection()) {
			return JdbcCommitMetadataTable.get(connection).countCommitTimestampsBetween(branchName, from, to);
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to access Commit Metadata Table!", e);
		}
	}

	@Override
	public int countCommitTimestamps() {
		String branchName = this.getBranchName();
		try (Connection connection = this.openConnection()) {
			return JdbcCommitMetadataTable.get(connection).countCommitTimestampsBetween(branchName, 0, Long.MAX_VALUE);
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
