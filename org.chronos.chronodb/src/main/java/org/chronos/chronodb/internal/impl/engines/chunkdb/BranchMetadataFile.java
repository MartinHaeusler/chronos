package org.chronos.chronodb.internal.impl.engines.chunkdb;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.chronos.chronodb.internal.impl.IBranchMetadata;
import org.chronos.common.exceptions.ChronosIOException;

public class BranchMetadataFile {

	// =================================================================================================================
	// STATIC FACTORY METHODS
	// =================================================================================================================

	public static BranchMetadataFile write(final IBranchMetadata metadata, final File file) {
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		BranchMetadataFile bmf = new BranchMetadataFile(file);
		bmf.writeMetadata(metadata);
		return bmf;
	}

	public static BranchMetadataFile read(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		return new BranchMetadataFile(file);
	}

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	private static final String KEY__BRANCH_NAME = "org.chronos.chronodb.branchmetadata.branchname";
	private static final String KEY__BRANCH_DIR = "org.chronos.chronodb.branchmetadata.branchdir";
	private static final String KEY__PARENT_BRANCH_NAME = "org.chronos.chronodb.branchmetadata.parentbranchname";
	private static final String KEY__BRANCHING_TIMESTAMP = "org.chronos.chronodb.branchmetadata.branchingtimestamp";

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final File file;
	private IBranchMetadata metadata;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	private BranchMetadataFile(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkArgument(file.exists(), "Precondition violation - argument 'file' must exist!");
		checkArgument(file.isFile(), "Precondition violation - argument 'file' must be a file (not a directory)!");
		checkArgument(file.canRead(), "Precondition violation - argument 'file' must be readable! Please check access permissions.");
		checkArgument(file.canWrite(), "Precondition violation - argument 'file' must be writeable! Please check access permissions.");
		this.file = file;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	public IBranchMetadata getMetadata() {
		if (this.metadata == null) {
			this.metadata = this.readMetadata();
		}
		return this.metadata;
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	private void writeMetadata(final IBranchMetadata metadata) {
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		Properties properties = new Properties();
		properties.setProperty(KEY__BRANCH_NAME, metadata.getName());
		properties.setProperty(KEY__BRANCH_DIR, metadata.getDirectoryName());
		if (metadata.getParentName() != null) {
			properties.setProperty(KEY__PARENT_BRANCH_NAME, metadata.getParentName());
		}
		properties.setProperty(KEY__BRANCHING_TIMESTAMP, String.valueOf(metadata.getBranchingTimestamp()));
		try (FileOutputStream fos = new FileOutputStream(this.file)) {
			properties.store(fos, "Chronos Branch Metadata. TREAT AS READ-ONLY!");
		} catch (IOException ioe) {
			throw new ChronosIOException("Failed to write to Branch Metadata File! See root cause for details.", ioe);
		}
	}

	private IBranchMetadata readMetadata() {
		try {
			Properties properties = new Properties();
			try (FileInputStream fis = new FileInputStream(this.file)) {
				properties.load(fis);
			}
			String branchName = properties.getProperty(KEY__BRANCH_NAME);
			String branchDir = properties.getProperty(KEY__BRANCH_DIR);
			String parentBranchName = properties.getProperty(KEY__PARENT_BRANCH_NAME);
			long branchingTimestamp = Long.parseLong(properties.getProperty(KEY__BRANCHING_TIMESTAMP));
			IBranchMetadata metadata = IBranchMetadata.create(branchName, parentBranchName, branchingTimestamp, branchDir);
			return metadata;
		} catch (IOException | NumberFormatException e) {
			throw new ChronosIOException("Failed to read branch metadata file! Check root cause for details.", e);
		}
	}

}
