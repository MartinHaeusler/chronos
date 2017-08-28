package org.chronos.chronodb.internal.impl.engines.chunkdb;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplUtils;
import org.chronos.common.exceptions.ChronosIOException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;

import com.google.common.collect.Maps;

public class ChronoChunkMetaData {

	private static final String INDEX_NAME = "chunkMetaData";

	private static final long CACHE_SIZE_BYTES = 1024 * 10; // 10KB

	private static final String KEY__VALID_FROM = "org.chronos.chonodb.chunk.validFrom";
	private static final String KEY__VALID_TO = "org.chronos.chonodb.chunk.validTo";
	private static final String KEY__BRANCH_NAME = "org.chronos.chronodb.chunk.branchName";

	private final File metaFile;
	private final Map<String, String> metaProperties;

	public ChronoChunkMetaData(final File metaFile) {
		checkNotNull(metaFile, "Precondition violation - argument 'metaFile' must not be NULL!");
		checkArgument(metaFile.exists(), "Precondition violation - argument 'metaFile' must exist!");
		checkArgument(metaFile.isFile(), "Precondition violation - argument 'metaFile' must be a regular file!");
		checkArgument(metaFile.canWrite(), "Precondition violation - argument 'metaFile' must be readable/writable!");
		checkArgument(metaFile.getName().endsWith(ChronoChunk.META_FILE_EXTENSION),
				"Precondition violation - argument 'metaFile' must end with '" + ChronoChunk.META_FILE_EXTENSION
						+ "'!");
		this.metaFile = metaFile;
		this.metaProperties = this.readMetaProperties();
	}

	private Map<String, String> readMetaProperties() {
		Database database = TuplUtils.openDatabase(this.metaFile, CACHE_SIZE_BYTES);
		try {
			Index index = database.openIndex(INDEX_NAME);
			Transaction tx = database.newTransaction();
			try {
				Map<String, String> resultMap = Maps.newHashMap();
				Cursor cursor = index.newCursor(tx);
				cursor.first();
				while (cursor.key() != null) {
					String key = TuplUtils.decodeString(cursor.key());
					String val = TuplUtils.decodeString(cursor.value());
					resultMap.put(key, val);
					cursor.next();
				}
				return resultMap;
			} finally {
				tx.reset();
			}
		} catch (IOException e) {
			throw new ChronosIOException("Failed to open index '" + INDEX_NAME + "'! See root cause for details.", e);
		} finally {
			TuplUtils.shutdownQuietly(database);
		}
	}

	public void flush() {
		Database database = TuplUtils.openDatabase(this.metaFile, CACHE_SIZE_BYTES);
		try {
			Index index = database.openIndex(INDEX_NAME);
			Transaction tx = database.newTransaction();
			try {
				for (Entry<String, String> entry : this.metaProperties.entrySet()) {
					byte[] key = TuplUtils.encodeString(entry.getKey());
					byte[] val = TuplUtils.encodeString(entry.getValue());
					index.store(tx, key, val);
				}
				tx.commit();
			} finally {
				tx.reset();
			}
		} catch (IOException e) {
			throw new ChronosIOException("Failed to open index '" + INDEX_NAME + "'! See root cause for details.", e);
		} finally {
			TuplUtils.shutdownQuietly(database);
		}
	}

	public void setValidFrom(final long validFrom) {
		checkArgument(validFrom >= 0,
				"Precondition violation - argument 'validFrom' must be greater than or equals to zero!");
		this.metaProperties.put(KEY__VALID_FROM, String.valueOf(validFrom));
	}

	public long getValidFrom() {
		return Long.parseLong(this.metaProperties.get(KEY__VALID_FROM));
	}

	public void setValidTo(final long validTo) {
		checkArgument(validTo >= 0,
				"Precondition violation - argument 'validTo' must be greater than or equals to zero!");
		this.metaProperties.put(KEY__VALID_TO, String.valueOf(validTo));
	}

	public long getValidTo() {
		return Long.parseLong(this.metaProperties.get(KEY__VALID_TO));
	}

	public Period getValidPeriod() {
		if (this.getValidTo() < Long.MAX_VALUE) {
			return Period.createRange(this.getValidFrom(), this.getValidTo());
		} else {
			return Period.createOpenEndedRange(this.getValidFrom());
		}
	}

	public String getBranchName() {
		return this.metaProperties.get(KEY__BRANCH_NAME);
	}

	public void setBranchName(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.metaProperties.put(KEY__BRANCH_NAME, branchName);
	}

	public File getMetaFile() {
		return this.metaFile;
	}

	public boolean isValid() {
		if (this.metaProperties.containsKey(KEY__VALID_FROM) == false) {
			return false;
		}
		if (this.metaProperties.containsKey(KEY__VALID_TO) == false) {
			return false;
		}
		return true;
	}
}
