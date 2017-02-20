package org.chronos.chronodb.internal.impl.engines.chunkdb;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

public class ChronoChunk {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	// file extensions
	public static final String CHUNK_FILE_EXTENSION = "cchunk";
	public static final String INDEX_FILE_EXTENSION = "cidx";
	public static final String META_FILE_EXTENSION = "cmeta";

	// file name format
	public static final String BASE_FORMAT_REGEX = "\\d{1,19}";
	public static final String CHUNK_FORMAT_REGEX = BASE_FORMAT_REGEX + "\\." + CHUNK_FILE_EXTENSION;
	public static final String META_FORMAT_REGEX = BASE_FORMAT_REGEX + "\\." + META_FILE_EXTENSION;
	public static final String INDEX_FORMAT_REGEX = BASE_FORMAT_REGEX + "\\." + INDEX_FILE_EXTENSION;

	// file name regex
	public static final Pattern DATA_FILENAME_PATTERN = Pattern.compile(CHUNK_FORMAT_REGEX);
	public static final Pattern META_FILENAME_PATTERN = Pattern.compile(META_FORMAT_REGEX);
	public static final Pattern INDEX_FILENAME_PATTERN = Pattern.compile(INDEX_FORMAT_REGEX);

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final long sequenceNumber;

	private final ChronoChunkMetaData metaData;
	private final File dataFile;
	private File indexFile;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public ChronoChunk(final ChronoChunkMetaData metaData, final File dataFile) {
		this(metaData, dataFile, null);
	}

	public ChronoChunk(final ChronoChunkMetaData metaData, final File dataFile, final File indexFile) {
		// check file integrity
		checkNotNull(metaData, "Precondition violation - argument 'metaFile' must not be NULL!");
		checkNotNull(dataFile, "Precondition violation - argument 'dataFile' must not be NULL!");
		checkArgument(dataFile.exists(), "Precondition violation - argument 'dataFile' must exist!");
		checkArgument(dataFile.isFile(), "Precondition violation - argument 'dataFile' must be a regular file!");
		checkArgument(dataFile.canWrite(), "Precondition violation - argument 'dataFile' must be accessible!");
		// check file names
		checkArgument(dataFile.getName().endsWith(CHUNK_FILE_EXTENSION),
				"Precondition violation - argument 'dataFile' must end with '" + CHUNK_FILE_EXTENSION + "'!");
		long metaFileSequenceNumber = this.getSequenceNumberOfFile(metaData.getMetaFile());
		long dataFileSequenceNumber = this.getSequenceNumberOfFile(dataFile);
		checkArgument(metaFileSequenceNumber == dataFileSequenceNumber,
				"Precondition violation - file sequence numbers do not match!");
		File metaFileParent = metaData.getMetaFile().getParentFile();
		File dataFileParent = dataFile.getParentFile();
		checkArgument(metaFileParent.equals(dataFileParent),
				"Precondition violation - files are not in same directory!");

		if (indexFile != null) {
			// check index file integrity
			checkArgument(indexFile.exists(), "Precondition violation - argument 'indexFile' must exist!");
			checkArgument(indexFile.isFile(), "Precondition violation - argument 'indexFile' must be a regular file!");
			checkArgument(indexFile.canWrite(), "Precondition violation - argument 'indexFile' must be accessible!");
			// check index file name
			checkArgument(indexFile.getName().endsWith(INDEX_FILE_EXTENSION),
					"Precondition violation - argument 'indexFile' must end with '" + INDEX_FILE_EXTENSION + "'!");
			long indexFileSequenceNumber = this.getSequenceNumberOfFile(indexFile);
			checkArgument(metaFileSequenceNumber == indexFileSequenceNumber,
					"Precondition violation - file sequence numbers do not match!");
			File indexFileParent = indexFile.getParentFile();
			checkArgument(metaFileParent.equals(indexFileParent),
					"Precondition violation - files are not in same directory!");
		}
		this.metaData = metaData;
		this.dataFile = dataFile;
		this.indexFile = indexFile;
		this.sequenceNumber = metaFileSequenceNumber;
	}

	// =================================================================================================================
	// GETTERS
	// =================================================================================================================

	public ChronoChunkMetaData getMetaData() {
		return this.metaData;
	}

	public File getDataFile() {
		return this.dataFile;
	}

	public File getIndexFile() {
		return this.indexFile;
	}

	public boolean hasIndexFile() {
		return this.indexFile != null;
	}

	public long getSequenceNumber() {
		return this.sequenceNumber;
	}

	public String getBranchName() {
		return this.getDataFile().getParentFile().getName();
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	public void createIndexFileIfNotExists() {
		if (this.indexFile != null) {
			return;
		}
		try {
			this.indexFile = new File(this.metaData.getMetaFile().getParent(),
					this.sequenceNumber + "." + INDEX_FILE_EXTENSION);
			this.indexFile.delete();
			this.indexFile.createNewFile();
		} catch (IOException e) {
			this.indexFile = null;
			throw new IllegalStateException("Unable to create index file.", e);
		}
	}

	public void deleteIndexFile() {
		if (this.hasIndexFile() == false) {
			return;
		}
		if (this.indexFile.exists()) {
			boolean deleted = this.indexFile.delete();
			if (!deleted) {
				throw new IllegalStateException("Unable to delete file '" + this.indexFile.getAbsolutePath() + "'!");
			}
		}
		this.indexFile = null;
	}

	// =================================================================================================================
	// INTERNAL HELPERS
	// =================================================================================================================

	private long getSequenceNumberOfFile(final File file) {
		String fileName = file.getName();
		return Long.parseLong(fileName.substring(0, fileName.lastIndexOf(".")));
	}

}
