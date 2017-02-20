package org.chronos.chronodb.internal.impl.engines.tupl;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Function;

import org.chronos.chronodb.api.exceptions.ChronoDBSerializationException;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.chronos.common.exceptions.ChronosIOException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.Index;

import com.google.common.collect.Maps;

public class TuplUtils {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	public static final String TUPL_DB_FILE_EXTENSION = "db";

	private static final Charset UTF8 = Charset.forName("UTF-8");

	/**
	 * Batch insert will outperform transactional insert if the number of values to be inserted is larger than this
	 * (measured) threshold.
	 */
	public static final int BATCH_INSERT_THRESHOLD = 25_000;

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	/**
	 * Encodes the given {@link String} into a byte array.
	 *
	 * @param data
	 *            The string to encode. May be <code>null</code>.
	 * @return The encoded string. Will be <code>null</code> if the input string is <code>null</code>.
	 */
	public static byte[] encodeString(final String data) {
		if (data == null) {
			return null;
		} else if (data.isEmpty()) {
			return new byte[0];
		} else {
			return data.getBytes(UTF8);
		}
	}

	/**
	 * Decodes the given raw byte array into a {@link String}.
	 *
	 * @param data
	 *            The data to decode. May be <code>null</code>.
	 * @return The decoded string. Will be <code>null</code> if the input array is <code>null</code>.
	 */
	public static String decodeString(final byte[] data) {
		if (data == null) {
			return null;
		} else if (data.length <= 0) {
			return "";
		} else {
			return new String(data, UTF8);
		}
	}

	/**
	 * Encodes the given {@link Boolean} into a byte array.
	 *
	 * @param value
	 *            The boolean to encode. May be <code>null</code>.
	 * @return The encoded boolean. Will be <code>null</code> if the input string is <code>null</code>.
	 */
	public static byte[] encodeBoolean(final Boolean value) {
		if (value == null) {
			return null;
		}
		byte[] encodedValue = new byte[1];
		if (value == true) {
			encodedValue[0] = (byte) 1;
		} else {
			encodedValue[0] = (byte) 0;
		}
		return encodedValue;
	}

	/**
	 * Decodes the given raw byte array into a {@link Boolean}.
	 *
	 * @param value
	 *            The value to decode. May be <code>null</code>.
	 * @return The decoded boolean. Will be <code>null</code> if the input array is <code>null</code>.
	 */
	public static Boolean decodeBoolean(final byte[] value) {
		if (value == null || value.length <= 0) {
			return null;
		}
		if (value.length != 1) {
			throw new ChronoDBSerializationException(
					"Cannot deserialize boolean - byte array contains more than one entry!");
		}
		byte byteValue = value[0];
		if (byteValue > 0) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Encodes the given {@link Long} into a byte array.
	 *
	 * @param value
	 *            The long value to encode. May be <code>null</code>.
	 * @return The encoded long value. Will be <code>null</code> if the input string is <code>null</code>.
	 */
	public static byte[] encodeLong(final Long value) {
		if (value == null) {
			return null;
		}
		byte[] bytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
		return bytes;
	}

	/**
	 * Decodes the given raw byte array into a {@link Long}.
	 *
	 * @param value
	 *            The value to decode. May be <code>null</code>.
	 * @return The decoded long value. Will be <code>null</code> if the input array is <code>null</code>.
	 */
	public static Long decodeLong(final byte[] value) {
		if (value == null) {
			return null;
		}
		return ByteBuffer.wrap(value).getLong();
	}

	/**
	 * Quietly {@linkplain Database#close() closes} the given {@link Database} instance, i.e. closing it without checked
	 * exceptions.
	 *
	 * <p>
	 * In general, prefer {@link #shutdownQuietly(Database)} over this method, as it is safer and produces cleaner
	 * on-disk representations. This method is reserved for "emergency exit" situations.
	 *
	 * <p>
	 * Any exceptions that arise during this process will be re-thrown, wrapped in an {@link IllegalStateException}.
	 *
	 * @param database
	 *            The database to close quietly. Must not be <code>null</code>.
	 *
	 * @throws IllegalStateException
	 *             Thrown if an exception occurs during the close process.
	 */
	public static void closeQuietly(final Database database) throws IllegalStateException {
		checkNotNull(database, "Precondition violation - argument 'database' must not be NULL!");
		try {
			database.close();
		} catch (Exception e) {
			throw new IllegalStateException("Failed to close database! See root cause for details.", e);
		}
	}

	/**
	 * Quietly {@linkplain Database#shutdown() shuts down} the given {@link Database} instance, i.e. shutting it down
	 * without checked exceptions.
	 *
	 * <p>
	 * Any exceptions that arise during this process will be re-thrown, wrapped in an {@link IllegalStateException}.
	 *
	 *
	 * @param database
	 *            The database to shut down quietly. Must not be <code>null</code>.
	 * @throws IllegalStateException
	 *             Thrown if an exception occurs during the shutdown process.
	 */
	public static void shutdownQuietly(final Database database) throws IllegalStateException {
		checkNotNull(database, "Precondition violation - argument 'database' must not be NULL!");
		try {
			database.shutdown();
		} catch (Exception e) {
			throw new IllegalStateException("Failed to shut down the database! See root cause for details.", e);
		}
	}

	/**
	 * Opens a new {@link Database} on the given {@link File}, respecting the given {@link ChronoDBConfiguration}.
	 *
	 * @param file
	 *            The file to open the database on. Must not be <code>null</code>.
	 * @param chronoConfig
	 *            The configuration to use for the new database instance. Must not be <code>null</code>.
	 * @return The newly created database instance. Never <code>null</code>.
	 *
	 * @throws IllegalStateException
	 *             Thrown if an error occurs while opening the database.
	 */
	public static Database openDatabase(final File file, final ChronoDBConfiguration chronoConfig) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkNotNull(chronoConfig, "Precondition violation - argument 'chronoConfig' must not be NULL!");
		return openDatabase(file, chronoConfig.getStorageBackendCacheMaxSize());
	}

	/**
	 * Opens a new {@link Database} on the given {@link File} with the given maximum cache size (in bytes).
	 *
	 * @param file
	 *            The file to use for the database instance. Must not be <code>null</code>.
	 * @param cacheMaxSizeBytes
	 *            The maximum number of bytes to allocate for the cache. Must be greater than zero.
	 * @return The newly created database instance. Never <code>null</code>.
	 *
	 * @throws IllegalStateException
	 *             Thrown if an error occurs while opening the database.
	 */
	public static Database openDatabase(final File file, final long cacheMaxSizeBytes) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkArgument(cacheMaxSizeBytes > 0, "Precondition violation - argument 'cacheMaxSizeBytes' must be positive!");
		try {
			DatabaseConfig config = new DatabaseConfig();
			config.baseFile(file);
			config.maxCacheSize(cacheMaxSizeBytes);
			Database database = Database.open(config);
			return database;
		} catch (IOException e) {
			throw new IllegalStateException(
					"Failed to open database on file '" + file.getAbsolutePath() + "'! See root cause for details.", e);
		}
	}

	public static <K extends Comparable<? super K>> void batchInsertWithoutCheckpoint(final TuplTransaction tx,
			final String indexName, final Map<K, byte[]> data, final Function<K, byte[]> keyMapper) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(data, "Precondition violation - argument 'data' must not be NULL!");
		checkNotNull(keyMapper, "Precondition violation - argument 'keyMapper' must not be NULL!");
		// sort entries
		SortedMap<K, byte[]> sortedEntries = Maps.newTreeMap();
		sortedEntries.putAll(data);
		// batch insert entries
		Index index = tx.getIndex(indexName);
		Cursor cursor = tx.newCursorOn(index);
		try {
			cursor.autoload(false);
			cursor.first();
			for (Map.Entry<K, byte[]> entry : sortedEntries.entrySet()) {
				cursor.findNearby(keyMapper.apply(entry.getKey()));
				cursor.store(entry.getValue());
			}
		} catch (IOException e) {
			throw new ChronosIOException("Failed to batch insert. See root cause for details.", e);
		} finally {
			cursor.reset();
		}
	}
}
