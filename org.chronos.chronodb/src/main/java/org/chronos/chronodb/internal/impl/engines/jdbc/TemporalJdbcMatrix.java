package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalDataMatrix;
import org.chronos.chronodb.internal.impl.jdbc.util.JdbcUtils;
import org.chronos.chronodb.internal.impl.stream.AbstractCloseableIterator;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey;
import org.chronos.chronodb.internal.util.KeySetModifications;

import com.google.common.collect.Iterators;

import javax.sql.DataSource;

public class TemporalJdbcMatrix extends AbstractTemporalDataMatrix {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final String tableName;
	private final DataSource dataSource;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public TemporalJdbcMatrix(final String keyspace, final long timestamp, final DataSource dataSource,
			final String tableName) {
		super(keyspace, timestamp);
		this.dataSource = dataSource;
		this.tableName = tableName;
		try (Connection connection = this.dataSource.getConnection()) {
			connection.setAutoCommit(false);
			boolean tableExists = JdbcUtils.tableExists(connection, this.tableName);
			if (tableExists == false) {
				JdbcMatrixTable.get(connection, tableName).ensureExists();
				connection.commit();
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not establish connection to storage backend", e);
		}
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public GetResult<byte[]> get(final long timestamp, final String key) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timsetamp' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		QualifiedKey qKey = QualifiedKey.create(this.getKeyspace(), key);
		try (Connection connection = this.dataSource.getConnection()) {
			return JdbcMatrixTable.get(connection, this.tableName).getRangedValueForKey(qKey, timestamp);
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to execute [GTR(" + key + ")] operation on backend", e);
		}
	}

	@Override
	public void put(final long timestamp, final Map<String, byte[]> contents) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(contents, "Precondition violation - argument 'contents' must not be NULL!");
		if (contents.isEmpty()) {
			return;
		}
		try (Connection connection = this.dataSource.getConnection()) {
			connection.setAutoCommit(false);
			for (Entry<String, byte[]> entry : contents.entrySet()) {
				String key = entry.getKey();
				byte[] value = entry.getValue();
				JdbcMatrixTable.get(connection, this.tableName).insert(timestamp, key, value);
			}
			connection.commit();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to execute [PUT] operation on backend", e);
		}
	}

	@Override
	public Iterator<Long> history(final long maxTime, final String key) {
		checkArgument(maxTime >= 0, "Precondition violation - argument 'maxTime' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		try (Connection connection = this.dataSource.getConnection()) {
			return JdbcMatrixTable.get(connection, this.tableName).getKeyHistoryTimestamps(key, maxTime);
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to execute [HISTORY(" + key + ")] operation on backend",
					e);
		}
	}

	@Override
	public void insertEntries(final Set<UnqualifiedTemporalEntry> entries) {
		checkNotNull(entries, "Precondition violation - argument 'entries' must not be NULL!");
		if (entries.isEmpty()) {
			return;
		}
		try (Connection connection = this.dataSource.getConnection()) {
			for (UnqualifiedTemporalEntry entry : entries) {
				UnqualifiedTemporalKey tk = entry.getKey();
				String actualKey = tk.getKey();
				long timestamp = tk.getTimestamp();
				byte[] value = entry.getValue();
				JdbcMatrixTable.get(connection, this.tableName).insert(timestamp, actualKey, value);
			}
			connection.commit();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to execute [INSERT] operation on backend", e);
		}
	}

	@Override
	public CloseableIterator<UnqualifiedTemporalEntry> allEntriesIterator(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		return new AllEntriesIterator(timestamp);
	}

	@Override
	public long lastCommitTimestamp(final String key) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		try (Connection connection = this.dataSource.getConnection()) {
			// TODO PERFORMANCE JDBC: this request should be batched on all keys for blind overwrite protection.
			return JdbcMatrixTable.get(connection, this.tableName).getLastCommitTimestamp(key);
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to execute [LAST COMMIT] operation on backend", e);
		}
	}

	@Override
	public void rollback(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		try (Connection connection = this.dataSource.getConnection()) {
			JdbcMatrixTable table = JdbcMatrixTable.get(connection, this.tableName);
			table.deleteWhereTimestampGreaterThan(timestamp);
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to execute [ROLLBACK] operation on backend", e);
		}
	}

	@Override
	public Iterator<TemporalKey> getModificationsBetween(final long timestampLowerBound,
			final long timestampUpperBound) {
		checkArgument(timestampLowerBound >= 0,
				"Precondition violation - argument 'timestampLowerBound' must not be negative!");
		checkArgument(timestampUpperBound >= 0,
				"Precondition violation - argument 'timestampUpperBound' must not be negative!");
		checkArgument(timestampLowerBound <= timestampUpperBound,
				"Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
		try (Connection connection = this.dataSource.getConnection()) {
			Set<Pair<String, Long>> set = JdbcMatrixTable.get(connection, this.tableName)
					.getModificationsBetween(timestampLowerBound, timestampUpperBound);
			Iterator<Pair<String, Long>> pairIterator = set.iterator();
			return Iterators.transform(pairIterator,
					pair -> TemporalKey.create(pair.getValue(), this.getKeyspace(), pair.getKey()));
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to execute [MODIFICATIONS BETWEEN] operation on backend",
					e);
		}
	}

	@Override
	public KeySetModifications keySetModifications(final long timestamp) {
		try (Connection connection = this.dataSource.getConnection()) {
			JdbcMatrixTable matrixTable = JdbcMatrixTable.get(connection, this.getTableName());
			return matrixTable.keySetModifications(timestamp);
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to execute [KEY SET MODS] operation on backend", e);
		}
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	public String getTableName() {
		return this.tableName;
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private class AllEntriesIterator extends AbstractCloseableIterator<UnqualifiedTemporalEntry> {

		private Connection connection;
		private final CloseableIterator<UnqualifiedTemporalEntry> entries;

		public AllEntriesIterator(final long maxTimestamp) {
			try {
				String tableName = TemporalJdbcMatrix.this.getTableName();
				this.connection = TemporalJdbcMatrix.this.dataSource.getConnection();
				this.entries = JdbcMatrixTable.get(this.connection, tableName).getEntriesBefore(maxTimestamp);
			} catch (SQLException e) {
				throw new ChronoDBStorageBackendException("Failed to access matrix table!", e);
			}
		}

		@Override
		protected boolean hasNextInternal() {
			return this.entries.hasNext();
		}

		@Override
		public UnqualifiedTemporalEntry next() {
			return this.entries.next();
		}

		@Override
		protected void closeInternal() {
			Exception exception = null;
			// first close the internal entries iterator
			try {
				this.entries.close();
			} catch (Exception e) {
				exception = e;
			}
			// then, close the connection
			try {
				this.connection.close();
			} catch (Exception e) {
				exception = e;
			}
			if (exception != null) {
				throw new ChronoDBStorageBackendException("Failed to close data stream!", exception);
			}
		}

	}
}
