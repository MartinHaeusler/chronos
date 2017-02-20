package org.chronos.chronodb.internal.impl.engines.chunkdb.index;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.internal.util.MultiMapUtil;
import org.chronos.common.exceptions.ChronosIOException;
import org.chronos.common.serialization.KryoManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

/**
 * This class is the in-memory representation of the data that is persisted inside an index chunk file.
 *
 * <p>
 * The primary content of this class is a list of index documents, alongside a bit of metadata (e.g. the indexers and the branch name). Note that this is a bare-bones data class; operations do not perform any consistency-insuring tasks.
 *
 * <p>
 * It provides two static methods for {@linkplain #loadFromFile(File) loading} and {@linkplain #persistToFile(File, DocumentChunkIndexData) persisting} instances from/to a file.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class DocumentChunkIndexData {

	// =====================================================================================================================
	// LOADING & SAVING
	// =====================================================================================================================

	/**
	 * Writes the given chunk index data into the given file.
	 *
	 * @param file
	 *            The file to write the data into. Must not be <code>null</code>. Must refer to a file (not a directory) and must be writable. It will be fully overwritten!
	 * @param chunkIndexData
	 *            The data to write into the file. Must not be <code>null</code>.
	 *
	 * @throws ChronosIOException
	 *             thrown if the writing process failed for any reason.
	 */
	public static void persistToFile(final File file, final DocumentChunkIndexData chunkIndexData) {
		try {
			if (file.exists()) {
				boolean deleted = file.delete();
				if (!deleted) {
					throw new IOException("Could not delete existing index chunk file!");
				}
			}
			boolean created = file.createNewFile();
			if (!created) {
				throw new IOException("Could not create index chunk file!");
			}
			ChunkIndexSeal seal = new ChunkIndexSeal();
			KryoManager.serializeObjectsToFile(file, chunkIndexData, seal);
		} catch (IOException e) {
			throw new ChronosIOException("Failed to persist chunk index!", e);
		}
	}

	/**
	 * Attempts to load the {@link DocumentChunkIndexData} contained in the given file.
	 *
	 * @param file
	 *            The file to read. Must not be <code>null</code>. Must point to an existing file (not a directory) that is readable.
	 * @return The index data object, or <code>null</code> if the file could not be read or is corrupted.
	 */
	public static DocumentChunkIndexData loadFromFile(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkArgument(file.exists(), "Precondition violation - argument 'file' must refer to an existing file!");
		checkArgument(file.isFile(),
				"Precondition violation - argument 'file' must point to a file (not a directory)!");
		checkArgument(file.canRead(), "Precondition violation - argument 'file' must be readable!");
		try {
			// attempt the deserialization. It will throw a ChronosIOException if there are structural problems.
			List<Object> fileContents = KryoManager.deserializeObjectsFromFile(file);
			// if the file is intact and valid, there should be two objects:
			// - the index data (the actual payload content)
			// - a seal object (to verify that the file has been fully written)
			if (fileContents.size() != 2) {
				// either file is empty, or not sealed, or somehow corrupt. Either way, it's useless.
				return null;
			}
			// check that the payload data and the seal are present
			Object firstObject = fileContents.get(0);
			Object secondObject = fileContents.get(1);
			if (firstObject == null || firstObject instanceof DocumentChunkIndexData == false) {
				// whatever the file contains, it's not an index...
				return null;
			}
			if (secondObject == null || secondObject instanceof ChunkIndexSeal == false) {
				// the file is not sealed, we can't trust it
				return null;
			}
			// everything ok
			return (DocumentChunkIndexData) firstObject;
		} catch (ChronosIOException e) {
			// we couldn't load the file for some reason, making it useless
			return null;
		}
	}

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	// metadata section
	private String branchName = null;
	private Map<String, Set<ChronoIndexer>> propertyNameToIndexers;

	// data section
	private List<ChunkDbIndexDocumentData> documents;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public DocumentChunkIndexData() {
		this.propertyNameToIndexers = Maps.newHashMap();
		this.documents = Lists.newArrayList();
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	public String getBranchName() {
		return this.branchName;
	}

	public void setBranchName(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.branchName = branchName;
	}

	public void setIndexers(final Map<String, Set<ChronoIndexer>> propertyNameToIndexers) {
		checkNotNull(propertyNameToIndexers,
				"Precondition violation - argument 'propertyNameToIndexers' must not be NULL!");
		this.propertyNameToIndexers = MultiMapUtil.copy(propertyNameToIndexers);
	}

	public void setIndexers(final SetMultimap<String, ChronoIndexer> propertyNameToIndexers) {
		checkNotNull(propertyNameToIndexers,
				"Precondition violation - argument 'propertyNameToIndexers' must not be NULL!");
		this.propertyNameToIndexers = MultiMapUtil.copyToMap(propertyNameToIndexers);
	}

	public void clearIndexers() {
		this.propertyNameToIndexers.clear();
	}

	public Set<String> getIndexedProperties() {
		return Collections.unmodifiableSet(this.propertyNameToIndexers.keySet());
	}

	public SetMultimap<String, ChronoIndexer> getIndexersByPropertyName() {
		return MultiMapUtil.copyToMultimap(this.propertyNameToIndexers);
	}

	public List<ChunkDbIndexDocumentData> getIndexDocuments() {
		return Collections.unmodifiableList(this.documents);
	}

	public void addIndexDocument(final ChunkDbIndexDocumentData document) {
		checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
		this.documents.add(document);
	}

	public void addIndexDocuments(final Collection<? extends ChunkDbIndexDocumentData> documents) {
		checkNotNull(documents, "Precondition violation - argument 'documents' must not be NULL!");
		this.documents.addAll(documents);
	}

	public void removeIndexDocument(final ChunkDbIndexDocumentData document) {
		checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
		this.documents.remove(document);
	}

	public void removeIndexDocuments(final Collection<? extends ChunkDbIndexDocumentData> documents) {
		checkNotNull(documents, "Precondition violation - argument 'documents' must not be NULL!");
		this.documents.removeAll(documents);
	}

	public void clearIndexDocuments() {
		this.documents.clear();
	}

	public void deleteIndexContents(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		this.documents.removeIf(doc -> doc.getIndexName().equals(indexName));
	}

}
