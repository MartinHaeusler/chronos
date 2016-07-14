package org.chronos.chronodb.internal.impl.engines.jdbc;

import static com.google.common.base.Preconditions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.api.exceptions.JdbcTableException;
import org.chronos.chronodb.internal.impl.jdbc.table.DefaultJdbcTable;
import org.chronos.chronodb.internal.impl.jdbc.table.IndexDeclaration;
import org.chronos.chronodb.internal.impl.jdbc.table.TableColumn;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

/**
 * The indexers table is intended to store the "index name to indexers" mapping for each branch.
 *
 * <p>
 * The indexers table has the following schema:
 *
 * <pre>
 * +-------------+--------------+--------------+----------------+
 * |             | id           | indexname    | indexerBlob    |
 * +-------------+--------------+--------------+----------------+
 * | TYPEBOUND   | VARCHAR(255) | VARCHAR(255) | VARBINARY(MAX) |
 * | CONSTRAINTS | PRIMARY KEY  | NOT NULL     | NOT NULL       |
 * +-------------+--------------+--------------+----------------+
 * </pre>
 *
 * There is exactly one instance of this table per {@link ChronoDB}. The name of this table is stored in the constant
 * {@link JdbcIndexerTable#NAME}.
 *
 * <p>
 * This class has <tt>default</tt> visibility (<tt>friendly</tt> visibility) on purpose. It is not intended to be used
 * outside of the package it resides in.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
class JdbcIndexerTable extends DefaultJdbcTable {

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	/**
	 * Grants access to the indexer table, using the given connection.
	 *
	 * @param connection
	 *            The connection to use for communication. Must not be <code>null</code>. Must be open.
	 * @return The indexer table. Never <code>null</code>.
	 */
	public static JdbcIndexerTable get(final Connection connection) {
		return new JdbcIndexerTable(connection);
	}

	// =================================================================================================================
	// TABLE DEFINITION
	// =================================================================================================================

	public static final String NAME = "ChronoIndexers";

	public static final String PROPERTY_ID = "id";

	public static final String TYPEBOUND_ID = "VARCHAR(255) NOT NULL";

	public static final String PROPERTY_INDEX_NAME = "indexname";

	public static final String TYPEBOUND_INDEX_NAME = "VARCHAR(255) NOT NULL";

	public static final String PROPERTY_INDEXER_BLOB = "indexerBlob";

	public static final String TYPEBOUND_INDEXER_BLOB = "VARBINARY(MAX) NOT NULL";

	public static final TableColumn[] COLUMNS = {
			//
			new TableColumn(PROPERTY_ID, TYPEBOUND_ID),
			//
			new TableColumn(PROPERTY_INDEX_NAME, TYPEBOUND_INDEX_NAME),
			//
			new TableColumn(PROPERTY_INDEXER_BLOB, TYPEBOUND_INDEXER_BLOB)
			//
	};

	public static final IndexDeclaration[] INDICES = {
			//
			new IndexDeclaration(NAME + "_IndexNameIndex", PROPERTY_INDEX_NAME)
			//
	};

	// =================================================================================================================
	// SQL STATEMENTS
	// =================================================================================================================

	public static final String SQL_INSERT_INDEXER = "INSERT INTO " + NAME + "  VALUES(?, ?, ?)";

	public static final String SQL_REMOVE_INDEX = "DELETE FROM " + NAME + " WHERE " + PROPERTY_INDEX_NAME + " = ?";

	public static final String SQL_REMOVE_ALL_INDEXERS = "DELETE FROM " + NAME + " WHERE 1 = 1";

	public static final String SQL_GET_INDEXERS = "SELECT " + PROPERTY_INDEX_NAME + ", " + PROPERTY_INDEXER_BLOB
			+ " FROM " + NAME;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected JdbcIndexerTable(final Connection connection) {
		super(connection);
	}

	// =================================================================================================================
	// API IMPLEMENTATION
	// =================================================================================================================

	@Override
	protected String getName() {
		return NAME;
	}

	@Override
	protected TableColumn[] getColumns() {
		return COLUMNS;
	}

	@Override
	protected IndexDeclaration[] getIndexDeclarations() {
		return INDICES;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	/**
	 * Inserts a row into the navigation table.
	 *
	 * @param indexName
	 *            The name of the index on which the indexer operates. Must not be <code>null</code>.
	 * @param indexer
	 *            The indexer to add to the table. Must not be <code>null</code>.
	 *
	 * @throws ChronoDBStorageBackendException
	 *             Thrown if a backend error occurs during the operation.
	 */
	public void insert(final String indexName, final ChronoIndexer indexer) throws ChronoDBStorageBackendException {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(indexer, "Precondition violation - argument 'indexer' must not be NULL!");
		byte[] serializedIndexer = serializeIndexer(indexer);
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_INSERT_INDEXER)) {
			// prepare the BLOB
			Blob blob = this.connection.createBlob();
			try {
				blob.setBytes(1, serializedIndexer);
				// fill the variables in the prepared statement
				pstmt.setString(1, UUID.randomUUID().toString());
				pstmt.setString(2, indexName);
				pstmt.setBlob(3, blob);
				// execute the statement
				pstmt.executeUpdate();
			} finally {
				// free the blob (if it exists)
				if (blob != null) {
					blob.free();
				}
			}
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Could not insert a row into Indexers Table!", e);
		}
	}

	/**
	 * Removes all indexers.
	 *
	 * <p>
	 * If there are no indexers, this method does nothing.
	 */
	public void removeAllIndexers() {
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_REMOVE_ALL_INDEXERS)) {
			pstmt.executeUpdate();
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Could not remove indexers!", e);
		}
	}

	/**
	 * Removes all indexers attached to the given index.
	 *
	 * <p>
	 * If there are no indexers on the given index (or the index does not exist), this method does nothing.
	 *
	 * @param indexName
	 *            The name of the index to remove all indexers for. Must not be <code>null</code>.
	 */
	public void removeAllIndexersOfIndex(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_REMOVE_INDEX)) {
			pstmt.setString(1, indexName);
			pstmt.executeUpdate();
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Could not remove indexers!", e);
		}
	}

	/**
	 * Returns all indexers.
	 *
	 * @return An immutable one-to-many mapping from index name to indexers associated with this index.
	 */
	public SetMultimap<String, ChronoIndexer> getIndexers() {
		SetMultimap<String, ChronoIndexer> resultMap = HashMultimap.create();
		try (PreparedStatement pstmt = this.connection.prepareStatement(SQL_GET_INDEXERS)) {
			try (ResultSet resultSet = pstmt.executeQuery()) {
				while (resultSet.next()) {
					String indexName = resultSet.getString(PROPERTY_INDEX_NAME);
					ChronoIndexer indexer = null;
					Blob blob = resultSet.getBlob(PROPERTY_INDEXER_BLOB);
					byte[] bytes = null;
					try {
						bytes = blob.getBytes(1, (int) blob.length());
					} finally {
						blob.free();
					}
					if (bytes != null) {
						indexer = deserializeIndexer(bytes);
					}
					if (indexName != null && indexer != null) {
						resultMap.put(indexName, indexer);
					}
				}
			}
		} catch (SQLException | JdbcTableException e) {
			throw new ChronoDBStorageBackendException("Could not get indexers!", e);
		}
		return Multimaps.unmodifiableSetMultimap(resultMap);
	}

	// =================================================================================================================
	// INTERNAL UTILITY METHODS
	// =================================================================================================================

	private static byte[] serializeIndexer(final ChronoIndexer indexer) {
		checkNotNull(indexer, "Precondition violation - argument 'indexer' must not be NULL!");
		Kryo kryo = new Kryo();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Output output = new Output(baos);
		kryo.writeClassAndObject(output, indexer);
		output.close();
		return baos.toByteArray();
	}

	private static ChronoIndexer deserializeIndexer(final byte[] serialForm) {
		checkNotNull(serialForm, "Precondition violation - argument 'serialForm' must not be NULL!");
		Kryo kryo = new Kryo();
		ByteArrayInputStream bais = new ByteArrayInputStream(serialForm);
		Input input = new Input(bais);
		ChronoIndexer indexer = (ChronoIndexer) kryo.readClassAndObject(input);
		input.close();
		return indexer;
	}

}
