package org.chronos.chronodb.internal.impl.tupl;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;

import org.chronos.chronodb.api.exceptions.ChronoDBCommitException;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplUtils;
import org.chronos.common.exceptions.ChronosIOException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;

public interface TuplTransaction extends AutoCloseable {

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public Database getDB();

	public Transaction getRawTx();

	// =====================================================================================================================
	// DEFAULT METHOD IMPLEMENTATIONS
	// =====================================================================================================================

	@Override
	public default void close() {
		this.rollback();
	}

	public default Index getIndex(final String indexName) {
		try {
			return this.getDB().openIndex(indexName);
		} catch (IOException e) {
			throw new ChronosIOException("Failed to open index '" + indexName + "'!", e);
		}
	}

	public default Cursor newCursorOn(final Index index) {
		checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
		return index.newCursor(this.getRawTx());
	}

	public default Cursor newCursorOn(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		Index index = this.getIndex(indexName);
		return this.newCursorOn(index);
	}

	public default void store(final String indexName, final byte[] key, final byte[] value) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		Index index = this.getIndex(indexName);
		try {
			index.store(this.getRawTx(), key, value);
		} catch (IOException e) {
			throw new ChronosIOException("Failed to store data in index! See root cause for details.", e);
		}
	}

	public default void store(final String indexName, final String key, final byte[] value) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		this.store(indexName, TuplUtils.encodeString(key), value);
	}

	public default void delete(final String indexName, final byte[] key) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		Index index = this.getIndex(indexName);
		try {
			index.delete(this.getRawTx(), key);
		} catch (IOException e) {
			throw new ChronosIOException("Failed to delete key in index! See root cause for details.", e);
		}
	}

	public default void delete(final String indexName, final String key) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		this.delete(indexName, TuplUtils.encodeString(key));
	}

	public default byte[] load(final String indexName, final byte[] key) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		try {
			return this.getIndex(indexName).load(this.getRawTx(), key);
		} catch (IOException e) {
			throw new ChronosIOException("Failed to access index! See root cause for details.", e);
		}
	}

	public default byte[] load(final String indexName, final String key) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		return this.load(indexName, TuplUtils.encodeString(key));
	}

	public default void commit() {
		try {
			this.getRawTx().commit();
		} catch (IOException e) {
			throw new ChronoDBCommitException("Failed to commit! See root cause for details.", e);
		}
	}

	public default void rollback() {
		try {
			this.getRawTx().exit();
		} catch (IOException e) {
			throw new ChronoDBCommitException("Failed to rollback transaction! See root cause for details.", e);
		}
	}

}
