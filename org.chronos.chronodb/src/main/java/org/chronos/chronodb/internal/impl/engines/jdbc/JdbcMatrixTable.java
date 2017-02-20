package org.chronos.chronodb.internal.impl.engines.jdbc;

import static org.chronos.common.logging.ChronoLogger.*;

import static com.google.common.base.Preconditions.*;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.jdbc.table.DefaultJdbcTable;
import org.chronos.chronodb.internal.impl.jdbc.table.IndexDeclaration;
import org.chronos.chronodb.internal.impl.jdbc.table.TableColumn;
import org.chronos.chronodb.internal.impl.jdbc.util.JdbcUtils;
import org.chronos.chronodb.internal.impl.jdbc.util.NamedParameterStatement;
import org.chronos.chronodb.internal.impl.stream.AbstractCloseableIterator;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey;
import org.chronos.chronodb.internal.util.KeySetModifications;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

/**
 * Matrix Tables are used by {@link ChronoDB} to store the actual temporal key-value data.
 *
 * <p>
 * The schema for a matrix table is as follows:
 *
 * <pre>
 * +-------------+--------------+----------+--------------+----------------+
 * |             | id           | time     | mapkey       | mapval         |
 * +-------------+--------------+----------+--------------+----------------+
 * | TYPEBOUND   | VARCHAR(255) | BIGINT   | VARCHAR(255) | VARBINARY(MAX) |
 * | CONSTRAINTS | PRIMARY KEY  | NOT NULL | NOT NULL     |                |
 * +-------------+--------------+----------+--------------+----------------+
 * | INDEXED     | YES          | YES      | YES          | NO             |
 * +-------------+--------------+----------+--------------+----------------+
 * </pre>
 *
 * There can be multiple instances of a Matrix Table per {@link ChronoDB}. They follow the naming format:<br>
 * <br>
 *
 * <code>
 * MATRIX_{UUID}
 * </code>
 *
 * <br>
 * <br>
 * ... where <code>{UUID}</code> is a {@link UUID} converted to a string, with dash characters ('-') replaced by
 * underscores ('_').
 *
 * <p>
 * This class has <tt>default</tt> visibility (<tt>friendly</tt> visibility) on purpose. It is not intended to be used
 * outside of the package it resides in.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
class JdbcMatrixTable extends DefaultJdbcTable {

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	/**
	 * Grants access to the matrix table with the given name, using the given connection.
	 *
	 * @param connection
	 *            The connection to use for communication. Must not be <code>null</code>. Must be open.
	 * @param tableName
	 *            The name of the matrix table to retrieve. Must not be <code>null</code>.
	 *
	 * @return The matrix table with the given name, using the given connection. Never <code>null</code>.
	 */
	public static JdbcMatrixTable get(final Connection connection, final String tableName) {
		return new JdbcMatrixTable(connection, tableName);
	}

	/**
	 * Generates and returns a random name for a matrix table.
	 *
	 * @return A random matrix table name. Never <code>null</code>.
	 */
	public static String generateRandomName() {
		return "MATRIX_" + UUID.randomUUID().toString().replace("-", "_");
	}

	// =================================================================================================================
	// SQL DEFINITIONS
	// =================================================================================================================

	public static final String PROPERTY_ID = "id";

	public static final String TYPEBOUND_ID = "VARCHAR(255) NOT NULL";

	public static final String PROPERTY_TIMESTAMP = "time";

	public static final String TYPEBOUND_TIMESTAMP = "BIGINT NOT NULL";

	public static final String PROPERTY_KEY = "mapkey";

	public static final String TYPEBOUND_KEY = "VARCHAR(255) NOT NULL";

	public static final String PROPERTY_VALUE = "mapval";

	public static final String TYPEBOUND_VALUE = "VARBINARY(MAX)";

	public static final TableColumn[] COLUMNS = {
			//
			new TableColumn(PROPERTY_ID, TYPEBOUND_ID),
			//
			new TableColumn(PROPERTY_TIMESTAMP, TYPEBOUND_TIMESTAMP),
			//
			new TableColumn(PROPERTY_KEY, TYPEBOUND_KEY),
			//
			new TableColumn(PROPERTY_VALUE, TYPEBOUND_VALUE)
			//
	};

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final String tableName;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected JdbcMatrixTable(final Connection connection, final String tableName) {
		super(connection);
		this.assertIsValidMatrixTableName(tableName);
		this.tableName = tableName;
	}

	// =================================================================================================================
	// API IMPLEMENTATION
	// =================================================================================================================

	@Override
	protected TableColumn[] getColumns() {
		return COLUMNS;
	}

	@Override
	protected IndexDeclaration[] getIndexDeclarations() {
		List<IndexDeclaration> indices = Lists.newArrayList();
		IndexDeclaration keyIndex = new IndexDeclaration(this.tableName + "_KeyIndex", PROPERTY_KEY);
		indices.add(keyIndex);
		IndexDeclaration timeIndex = new IndexDeclaration(this.tableName + "_TimeIndex", PROPERTY_TIMESTAMP);
		indices.add(timeIndex);
		return indices.toArray(new IndexDeclaration[0]);
	}

	@Override
	protected String getName() {
		return this.tableName;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	/**
	 * Generates and returns the SQL command for a temporal <i>get</i> operation.
	 *
	 * <p>
	 * This operation generates the SQL syntax for a {@link NamedParameterStatement} with <b>two parameters</b>:
	 * <ul>
	 * <li><b><code>${key}</code></b> -- The map key to search for
	 * <li><b><code>${timestamp}</code></b> -- The timestamp at which the search occurs
	 * </ul>
	 *
	 * The <b>result</b> of this query will have two columns:
	 * <ol>
	 * <li>{@link PROPERTY_VALUE}: The actual value (blob)
	 * <li>{@link PROPERTY_TIMESTAMP}: The actual timestamp where the given value was written
	 * </ol>
	 *
	 * @return The SQL for the named parameter statement, as specified above.
	 */
	private String generateSQLGetValue() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT t1.");
		sql.append(PROPERTY_VALUE);
		sql.append(", t1.");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" FROM ");
		sql.append(this.tableName);
		sql.append(" t1 WHERE t1.");
		sql.append(PROPERTY_KEY);
		sql.append(" = ${key} AND t1.");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" = ( SELECT MAX(t2.");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(") FROM ");
		sql.append(this.tableName);
		sql.append(" t2 WHERE t2.");
		sql.append(PROPERTY_KEY);
		sql.append(" = ${key} AND t2.");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" <= ${timestamp} )");
		return sql.toString();
	}

	private String generateSQLGetRangeValidUntil() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT tCeil.");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" FROM ");
		sql.append(this.tableName);
		sql.append(" AS tCeil WHERE tCeil.");
		sql.append(PROPERTY_KEY);
		sql.append(" = ${key} AND tCeil.");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" = ( SELECT MIN(tTemp.");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(") FROM ");
		sql.append(this.tableName);
		sql.append(" tTemp WHERE tTemp.");
		sql.append(PROPERTY_KEY);
		sql.append(" = ${key} AND tTemp.");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" > ${timestamp}  )");
		return sql.toString();
	}

	public GetResult<byte[]> getRangedValueForKey(final QualifiedKey qKey, final long timestamp) {
		checkNotNull(qKey, "Precondition violation - argument 'qKey' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		// Note: this operation is far too complex to be executed in a single query. We have to split it up.
		// first, perform a regular "get" call
		String sql = this.generateSQLGetValue();
		long floorTimestamp = -1;
		long ceilTimestamp = -1;
		byte[] value = null;
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("key", qKey.getKey());
			nStmt.setParameter("timestamp", timestamp);
			logTrace("[GTR] " + nStmt.toStringWithResolvedParameters());
			try (ResultSet resultSet = nStmt.executeQuery()) {
				if (resultSet.next()) {
					// we found an entry for the given key at (or before) the given timestamp
					floorTimestamp = resultSet.getLong(PROPERTY_TIMESTAMP);
					Blob blob = resultSet.getBlob(PROPERTY_VALUE);
					byte[] bytes = null;
					try {
						bytes = blob.getBytes(1, (int) blob.length());
					} finally {
						blob.free();
					}
					value = bytes;
				} else {
					// we have no entry for the given key at (or before) the given timestamp
					floorTimestamp = 0;
					value = null;
				}
				if (resultSet.next()) {
					throw new ChronoDBStorageBackendException(
							"[GTR(" + qKey + ", " + timestamp + ")] has multiple results for the same timestamp!");
				}
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException(
					"Could not perform [GTR(" + qKey + ", " + timestamp + ")]on Matrix Table '" + this.tableName + "'!",
					e);
		}
		// then, run the query for the "ceilTimestamp"
		sql = this.generateSQLGetRangeValidUntil();
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("key", qKey.getKey());
			nStmt.setParameter("timestamp", timestamp);
			try (ResultSet resultSet = nStmt.executeQuery()) {
				if (resultSet.next()) {
					// we found an entry for the given key after the given timestamp
					ceilTimestamp = resultSet.getLong(PROPERTY_TIMESTAMP);
				} else {
					// we did not find an entry for the given key after the given timestamp
					ceilTimestamp = Long.MAX_VALUE;
				}
				if (resultSet.next()) {
					throw new ChronoDBStorageBackendException(
							"[GTR(" + qKey + ", " + timestamp + ")] has multiple results for the same timestamp!");
				}
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException(
					"Could not perform [GTR(" + qKey + ", " + timestamp + ")]on Matrix Table '" + this.tableName + "'!",
					e);
		}
		// now we can construct the period in which our result is valid
		Period range = Period.createRange(floorTimestamp, ceilTimestamp);
		boolean foundSomething;
		if (range.getLowerBound() <= 0) {
			// we found no entry in our table
			foundSomething = false;
		} else {
			// we found an entry
			foundSomething = true;
		}
		if (foundSomething) {
			return GetResult.create(qKey, value, range);
		} else {
			return GetResult.createNoValueResult(qKey, range);
		}
	}

	/**
	 * Generates and returns the SQL <code>INSERT</code> command to insert a row into this Matrix Table.
	 *
	 * <p>
	 * This operation generates the SQL syntax for a <b>prepared statement</b> with <b>four parameters</b>:
	 * <ol>
	 * <li>The row id (primary key)
	 * <li>The timestamp at which the search occurs
	 * <li>The map key to insert
	 * <li>The value (BLOB) to insert
	 * </ol>
	 *
	 * The repeated usage of the map key parameter is introduced by a SQL Sub-SELECT clause and the lack of named
	 * parameters in plain JDBC.
	 *
	 * @return The SQL prepared statement, as specified above.
	 */
	private String generateSQLInsert() {
		StringBuilder sql = new StringBuilder();
		sql.append("INSERT INTO ");
		sql.append(this.tableName);
		sql.append(" VALUES (?,?,?,?)");
		return sql.toString();
	}

	/**
	 * Performs an insert operation into this Matrix Table.
	 *
	 * @param timestamp
	 *            The timestamp at which to insert the key-value pair. Must not be negative.
	 * @param mapKey
	 *            The key of the key-value pair to insert. Must not be <code>null</code>.
	 * @param value
	 *            The value of the key-value pair to insert. May be <code>null</code>.
	 *
	 * @throws ChronoDBStorageBackendException
	 *             Thrown if an exception occurs in the storage backend during the execution of this operation.
	 */
	public void insert(final long timestamp, final String mapKey, final byte[] value)
			throws ChronoDBStorageBackendException {
		checkArgument(timestamp >= 0,
				"Precondition violation - argument 'timestamp' must be >= 0 (value: " + timestamp + ")!");
		checkNotNull(mapKey, "Precondition violation - argument 'mapKey' must not be NULL!");
		// first of all, remove the entry if it exists (in case of incremental update, that can happen)
		String sqlRemove = "DELETE FROM " + this.tableName + " WHERE " + PROPERTY_TIMESTAMP + " = ? AND " + PROPERTY_KEY
				+ " = ?";
		try (PreparedStatement pstmt = this.connection.prepareStatement(sqlRemove)) {
			pstmt.setLong(1, timestamp);
			pstmt.setString(2, mapKey);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not perform INSERT into Matrix Table!", e);
		}
		String sql = this.generateSQLInsert();
		try (PreparedStatement pstmt = this.connection.prepareStatement(sql)) {
			String primaryKey = UUID.randomUUID().toString();
			Blob blob = this.connection.createBlob();
			if (value != null) {
				blob.setBytes(1, value);
			} else {
				blob.setBytes(1, new byte[0]);
			}
			pstmt.setString(1, primaryKey);
			pstmt.setLong(2, timestamp);
			pstmt.setString(3, mapKey);
			pstmt.setBlob(4, blob);
			logTrace("[PUT] " + JdbcUtils.resolvePreparedStatement(sql, primaryKey, timestamp, mapKey, value));
			pstmt.executeUpdate();
			pstmt.close();
			blob.free();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Could not perform INSERT into Matrix Table!", e);
		}
	}

	/**
	 * Generates the SQL <code>SELECT</code> command for a <i>key history</i> query on this Matrix Table.
	 *
	 * <p>
	 * This operation generates the SQL syntax for a <b>prepared statement</b> with <b>two parameters</b>:
	 * <ol>
	 * <li>The map key to retrieve the history for
	 * <li>The timestamp serving as the upper limit for the history (inclusive)
	 * </ol>
	 *
	 * @return The SQL <code>SELECT</code> command for a key history query.
	 */
	private String generateSQLGetKeyHistory() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT DISTINCT ");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" FROM ");
		sql.append(this.tableName);
		sql.append(" WHERE ");
		sql.append(PROPERTY_KEY);
		sql.append(" = ? AND ");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" <= ?");
		return sql.toString();
	}

	/**
	 * Queries this Matrix Table for the key change timestamps (key history) of the given key.
	 *
	 * @param mapKey
	 *            The map key to get the change timestamps for. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp which serves as an upper limit for the history search (inclusive). Must not be negative.
	 * @return An iterator over the change timestamps. May be empty, but never <code>null</code>.
	 * @throws ChronoDBStorageBackendException
	 *             Thrown if an exception occurs in the storage backend during the execution of this operation.
	 */
	public Iterator<Long> getKeyHistoryTimestamps(final String mapKey, final long timestamp)
			throws ChronoDBStorageBackendException {
		checkNotNull(mapKey, "Precondition violation - argument 'mapKey' must not be NULL!");
		checkArgument(timestamp >= 0,
				"Precondition violation - argument 'timestamp' must be >= 0 (value: " + timestamp + ")!");
		String sql = this.generateSQLGetKeyHistory();
		try (PreparedStatement stmt = this.connection.prepareStatement(sql)) {
			stmt.setString(1, mapKey);
			stmt.setLong(2, timestamp);
			logTrace("[HST] " + JdbcUtils.resolvePreparedStatement(sql, mapKey, timestamp));
			List<Long> changeTimes = Lists.newArrayList();
			try (ResultSet resultSet = stmt.executeQuery()) {
				while (resultSet.next()) {
					long changeTimestamp = resultSet.getLong(PROPERTY_TIMESTAMP);
					changeTimes.add(changeTimestamp);
				}
			}
			stmt.close();
			return changeTimes.iterator();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException(
					"Could not perform [HST(" + mapKey + ")] on Matrix Table '" + this.tableName + "'!", e);
		}
	}

	/**
	 * Generates the SQL <code>SELECT</code> command for a <i>last commit timestamp</i> query on this Matrix Table.
	 *
	 * <p>
	 * This operation generates the SQL syntax for a <b>prepared statement</b> with <b>one parameter</b>:
	 * <ol>
	 * <li>The map key to retrieve the history for
	 * </ol>
	 *
	 * @return The SQL <code>SELECT</code> command for a last commit timestamp query.
	 */
	private String generateSQLGetLastCommitTimestamp() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT MAX(");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(") FROM ");
		sql.append(this.tableName);
		sql.append(" WHERE ");
		sql.append(PROPERTY_KEY);
		sql.append(" = ?");
		return sql.toString();
	}

	/**
	 * Queries this Matrix Table for the latest timestamp at which the given key was changed.
	 *
	 * @param key
	 *            The key to get the last commit timestamp for. Must not be <code>null</code>.
	 * @return The timestamp at which the last commit occurred, or a negative value if there has never been any commit
	 *         on the given key.
	 */
	public long getLastCommitTimestamp(final String key) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		String sql = this.generateSQLGetLastCommitTimestamp();
		try (PreparedStatement pstmt = this.connection.prepareStatement(sql)) {
			pstmt.setString(1, key);
			logTrace("[LCT] " + JdbcUtils.resolvePreparedStatement(sql, key));
			try (ResultSet resultSet = pstmt.executeQuery()) {
				if (resultSet.next()) {
					// return the commit timestamp
					return resultSet.getLong(1);
				} else {
					// there are no commits on this key
					return -1;
				}
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException(
					"Could not perform [LCT(" + key + ")] on Matrix Table '" + this.tableName + "'!", e);
		}
	}

	/**
	 * Generates the SQL <code>SELECT</code> command for a <i>all entries</i> query on this Matrix Table.
	 *
	 * <p>
	 * This operation generates the SQL syntax for a <b>prepared statement</b> with <b>one parameter</b> which specifies
	 * the maximum timestamp to consider.
	 *
	 * @return The SQL <code>SELECT</code> command for a key set query.
	 */
	private String generateSQLGetEntriesBefore() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(", ");
		sql.append(PROPERTY_KEY);
		sql.append(", ");
		sql.append(PROPERTY_VALUE);
		sql.append(" FROM ");
		sql.append(this.tableName);
		sql.append(" WHERE ");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" <= ?");
		return sql.toString();
	}

	/**
	 * Queries this Matrix Table to return all entries before the given timestamp.
	 *
	 * <p>
	 * Note that this will include entries which have the same key, but different timestamps.
	 *
	 * @param maxTimestamp
	 *            The maximum timestamp to consider. Larger timestamps will be ignored. Must be >= 0.
	 *
	 * @return The set of entries that have been inserted before the given timestamp. May be empty, but never
	 *         <code>null</code>.
	 */
	public CloseableIterator<UnqualifiedTemporalEntry> getEntriesBefore(final long maxTimestamp) {
		checkArgument(maxTimestamp >= 0,
				"Precondition violation - argument 'maxTimestamp' must be >= 0 (value: " + maxTimestamp + ")!");
		String sql = this.generateSQLGetEntriesBefore();
		try {
			PreparedStatement pstmt = this.connection.prepareStatement(sql);
			pstmt.setLong(1, maxTimestamp);
			logTrace("[AEN] " + JdbcUtils.resolvePreparedStatement(sql, maxTimestamp));
			return new EntriesBeforeIterator(pstmt);
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException(
					"Could not perform [AEN] on Matrix Table '" + this.tableName + "'!", e);
		}
	}

	private String generateSQLDeleteWhereTimestampsGreaterThan() {
		StringBuilder sql = new StringBuilder();
		sql.append("DELETE FROM ");
		sql.append(this.tableName);
		sql.append(" WHERE ");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" > ?");
		return sql.toString();
	}

	public void deleteWhereTimestampGreaterThan(final long timestamp) {
		String sql = this.generateSQLDeleteWhereTimestampsGreaterThan();
		try (PreparedStatement pstmt = this.connection.prepareStatement(sql)) {
			pstmt.setLong(1, timestamp);
			logTrace("[RBK] " + JdbcUtils.resolvePreparedStatement(sql, timestamp));
			pstmt.executeUpdate();
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException(
					"Could not perform update on Matrix Table '" + this.tableName + "'!", e);
		}
	}

	private String generateSQLGetModificationsBetween() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");
		sql.append(PROPERTY_KEY);
		sql.append(", ");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" FROM ");
		sql.append(this.tableName);
		sql.append(" WHERE ");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" >= ${lowerBound} AND ");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" <= ${upperBound}");
		return sql.toString();
	}

	public Set<Pair<String, Long>> getModificationsBetween(final long timestampLowerBound,
			final long timestampUpperBound) {
		checkArgument(timestampLowerBound >= 0,
				"Precondition violation - argument 'timestampLowerBound' must not be negative!");
		checkArgument(timestampUpperBound >= 0,
				"Precondition violation - argument 'timestampUpperBound' must not be negative!");
		checkArgument(timestampLowerBound <= timestampUpperBound,
				"Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
		String sql = this.generateSQLGetModificationsBetween();
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("lowerBound", timestampLowerBound);
			nStmt.setParameter("upperBound", timestampUpperBound);
			Set<Pair<String, Long>> result = Sets.newHashSet();
			try (ResultSet resultSet = nStmt.executeQuery()) {
				while (resultSet.next()) {
					String key = resultSet.getString(PROPERTY_KEY);
					long timestamp = resultSet.getLong(PROPERTY_TIMESTAMP);
					Pair<String, Long> pair = Pair.of(key, timestamp);
					result.add(pair);
				}
			}
			return result;
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to read from Matrix Table '" + this.tableName + "'!", e);
		}
	}

	private String generateSQLGetKeySetAdditions() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");
		sql.append(PROPERTY_KEY);
		sql.append(", ");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" FROM ");
		sql.append(this.tableName);
		sql.append(" WHERE ");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" <= ${upperBound} AND LENGTH(");
		sql.append(PROPERTY_VALUE);
		sql.append(") > 0");
		return sql.toString();
	}

	private String generateSQLGetKeySetRemovals() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");
		sql.append(PROPERTY_KEY);
		sql.append(", ");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" FROM ");
		sql.append(this.tableName);
		sql.append(" WHERE ");
		sql.append(PROPERTY_TIMESTAMP);
		sql.append(" <= ${upperBound} AND LENGTH(");
		sql.append(PROPERTY_VALUE);
		sql.append(") <= 0");
		return sql.toString();
	}

	public KeySetModifications keySetModifications(final long maxTimestamp) {
		checkArgument(maxTimestamp >= 0,
				"Precondition violation - argument 'maxTimestamp' must be greater than or equal to zero!");
		// multi map for storing additions/removals in ascending order
		Multimap<Long, Pair<String, Boolean>> modificationsMap = TreeMultimap.create();

		// get the additions
		String sql = this.generateSQLGetKeySetAdditions();
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("upperBound", maxTimestamp);
			try (ResultSet resultSet = nStmt.executeQuery()) {
				while (resultSet.next()) {
					String key = resultSet.getString(PROPERTY_KEY);
					long timestamp = resultSet.getLong(PROPERTY_TIMESTAMP);
					modificationsMap.put(timestamp, Pair.of(key, true));
				}
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to read from Matrix Table '" + this.tableName + "'!", e);
		}

		// get the removals
		sql = this.generateSQLGetKeySetRemovals();
		try (NamedParameterStatement nStmt = new NamedParameterStatement(this.connection, sql)) {
			nStmt.setParameter("upperBound", maxTimestamp);
			try (ResultSet resultSet = nStmt.executeQuery()) {
				while (resultSet.next()) {
					String key = resultSet.getString(PROPERTY_KEY);
					long timestamp = resultSet.getLong(PROPERTY_TIMESTAMP);
					modificationsMap.put(timestamp, Pair.of(key, false));
				}
			}
		} catch (SQLException e) {
			throw new ChronoDBStorageBackendException("Failed to read from Matrix Table '" + this.tableName + "'!", e);
		}

		// combine them
		Set<String> additions = Sets.newHashSet();
		Set<String> removals = Sets.newHashSet();
		// iteration is done in ascending timestamp order
		for (Pair<String, Boolean> entry : modificationsMap.values()) {
			String key = entry.getLeft();
			Boolean isAddition = entry.getRight();
			if (isAddition) {
				additions.add(key);
				removals.remove(key);
			} else {
				additions.remove(key);
				removals.add(key);
			}
		}
		return new KeySetModifications(additions, removals);
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	/**
	 * Asserts that the given table name is a valid matrix table name.
	 *
	 * <p>
	 * If the table name matches the syntax, this method does nothing. Otherwise, an exception is thrown.
	 *
	 * @param tableName
	 *            The table name to verify.
	 *
	 * @throws NullPointerException
	 *             Thrown if the table name is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             Thrown if the table name is no syntactically valid matrix table name.
	 *
	 * @see #isValidMatrixTableName(String)
	 */
	private void assertIsValidMatrixTableName(final String tableName) {
		if (tableName == null) {
			throw new IllegalArgumentException("NULL is no valid Matrix Table name!");
		}
		if (this.isValidMatrixTableName(tableName) == false) {
			throw new IllegalArgumentException("The table name '" + tableName + "' is no valid Matrix Table name!");
		}
	}

	/**
	 * Checks if the given string is a valid matrix table name.
	 *
	 * @param tableName
	 *            The table name to check. May be <code>null</code>.
	 * @return <code>true</code> if the given table name is a valid matrix table name, or <code>false</code> if it is
	 *         syntactically invalid or <code>null</code>.
	 */
	private boolean isValidMatrixTableName(final String tableName) {
		if (tableName == null) {
			return false;
		}
		String tablenNameRegex = "MATRIX_[a-zA-Z0-9_]+";
		return tableName.matches(tablenNameRegex);
	}

	private static class EntriesBeforeIterator extends AbstractCloseableIterator<UnqualifiedTemporalEntry> {

		private final PreparedStatement statement;
		private final ResultSet resultSet;

		private UnqualifiedTemporalEntry current;
		private UnqualifiedTemporalEntry next;

		public EntriesBeforeIterator(final PreparedStatement statement) {
			checkNotNull(statement, "Precondition violation - argument 'statement' must not be NULL!");
			try {
				this.statement = statement;
				this.resultSet = statement.executeQuery();
				// fill the "next" pointer
				this.moveNextInResultSet();
			} catch (SQLException e) {
				throw new ChronoDBStorageBackendException("Failed to read matrix data!", e);
			}
		}

		@Override
		public UnqualifiedTemporalEntry next() {
			if (this.hasNext() == false) {
				throw new NoSuchElementException();
			}
			if (this.isClosed()) {
				throw new IllegalStateException("Iterator was already closed!");
			}
			this.moveNextInResultSet();
			return this.current;
		}

		@Override
		protected boolean hasNextInternal() {
			return this.next != null;
		}

		@Override
		protected void closeInternal() {
			try {
				this.resultSet.close();
			} catch (SQLException e) {
				throw new ChronoDBStorageBackendException("Failed to close SQL connection!", e);
			}
			try {
				this.statement.close();
			} catch (SQLException e) {
				throw new ChronoDBStorageBackendException("Failed to close SQL connection!", e);
			}
		}

		// =====================================================================================================================
		// INTERNAL HELPER METHODS
		// =====================================================================================================================

		private void moveNextInResultSet() {
			try {
				this.current = this.next;
				boolean hasNext = this.resultSet.next();
				if (hasNext == false) {
					this.next = null;
				} else {
					this.next = this.readAndConvertRowFromResultSet();
				}
			} catch (SQLException e) {
				throw new ChronoDBStorageBackendException("Failed to read from SQL result set!", e);
			}
		}

		private UnqualifiedTemporalEntry readAndConvertRowFromResultSet() {
			try {
				long timestamp = this.resultSet.getLong(PROPERTY_TIMESTAMP);
				String key = this.resultSet.getString(PROPERTY_KEY);
				Blob valueBlob = this.resultSet.getBlob(PROPERTY_VALUE);
				byte[] value = null;
				try {
					value = valueBlob.getBytes(1, (int) valueBlob.length());
				} finally {
					valueBlob.free();
				}
				if (value == null || value.length < 1) {
					value = null;
				}
				UnqualifiedTemporalKey unqualifiedKey = new UnqualifiedTemporalKey(key, timestamp);
				UnqualifiedTemporalEntry entry = new UnqualifiedTemporalEntry(unqualifiedKey, value);
				return entry;
			} catch (SQLException e) {
				throw new ChronoDBStorageBackendException("Failed to read from SQL result set!", e);
			}
		}

	}
}
