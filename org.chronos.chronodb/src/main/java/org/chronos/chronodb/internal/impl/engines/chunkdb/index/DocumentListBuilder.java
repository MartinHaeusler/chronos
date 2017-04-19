package org.chronos.chronodb.internal.impl.engines.chunkdb.index;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class DocumentListBuilder {

	private final List<ChunkDbIndexDocumentData> allDocuments;
	private final Map<String, SetMultimap<String, ChunkDbIndexDocumentData>> keyspaceToKeyToOpenDocuments;
	private boolean isClosed;

	public DocumentListBuilder() {
		this.allDocuments = Lists.newArrayList();
		this.keyspaceToKeyToOpenDocuments = Maps.newHashMap();
		this.isClosed = false;
	}

	/**
	 * Adds the given document to be managed by this builder.
	 *
	 * @param newDocument
	 *            The new document. Must not be <code>null</code>. Must have an open-ended validity range (i.e. must not
	 *            have been terminated yet).
	 *
	 * @throws IllegalStateException
	 *             Thrown if this builder is already closed, i.e. {@link #getAllDocumentsAndClose()} has already been
	 *             invoked on it.
	 */
	public void addDocument(final ChunkDbIndexDocumentData newDocument) {
		if (this.isClosed) {
			throw new IllegalStateException("This DocumentListBuilder instance is already closed!");
		}
		if (newDocument == null) {
			throw new NullPointerException("Precondition violation - argument 'newDocument' must not be NULL!");
		}
		if (newDocument.getValidToTimestamp() < Long.MAX_VALUE) {
			throw new IllegalArgumentException(
					"Precondition violation - argument 'newDocument' must not be terminated!");
		}
		this.allDocuments.add(newDocument);
		SetMultimap<String, ChunkDbIndexDocumentData> keyToOpenDocs = this.keyspaceToKeyToOpenDocuments
				.get(newDocument.getKeyspace());
		if (keyToOpenDocs == null) {
			keyToOpenDocs = HashMultimap.create();
			this.keyspaceToKeyToOpenDocuments.put(newDocument.getKeyspace(), keyToOpenDocs);
		}
		keyToOpenDocs.put(newDocument.getKey(), newDocument);
	}

	/**
	 * Terminates the validity of the given document at the given timestamp.
	 *
	 * <p>
	 * It is imperative that the given document was added before via {@link #addDocument(ChunkDbIndexDocumentData)}.
	 * Failing to do so will result in an {@link IllegalArgumentException} when calling this method.
	 *
	 * @param document
	 *            The document. Must not be <code>null</code>. It must have an open-ended validity range (i.e. it must
	 *            not have been terminated yet). Will be terminated by this method. Must have been added to this builder
	 *            via {@link #addDocument(ChunkDbIndexDocumentData)} first.
	 * @param timestamp
	 *            The timestamp at which to terminate the document validity. Must not be negative. Must be larger than
	 *            the document's {@linkplain ChunkDbIndexDocumentData#getValidFromTimestamp() valid-from} timestamp.
	 *
	 * @throws IllegalStateException
	 *             Thrown if this builder is already closed, i.e. {@link #getAllDocumentsAndClose()} has already been
	 *             invoked on it.
	 */
	public void terminateDocumentValidity(final ChunkDbIndexDocumentData document, final long timestamp) {
		if (this.isClosed) {
			throw new IllegalStateException("This DocumentListBuilder instance is already closed!");
		}
		if (document == null) {
			throw new NullPointerException("Precondition violation - argument 'document' must not be NULL!");
		}
		if (document.getValidToTimestamp() < Long.MAX_VALUE) {
			throw new IllegalArgumentException(
					"Precondition violation - argument 'document' must have open-ended validity!");
		}
		if (timestamp <= 0) {
			throw new IllegalArgumentException("Precondition violation - argument 'timestamp' must not be negative!");
		}
		// set the valid-to timestamp
		document.setValidToTimestamp(timestamp);
		// remove it from the open documents
		SetMultimap<String, ChunkDbIndexDocumentData> keyToOpenDocuments = this.keyspaceToKeyToOpenDocuments
				.get(document.getKeyspace());
		if (keyToOpenDocuments == null) {
			throw new IllegalArgumentException(
					"Precondition violation - the given 'document' is not managed by this builder!");
		}
		boolean removed = keyToOpenDocuments.remove(document.getKey(), document);
		if (removed == false) {
			throw new IllegalArgumentException(
					"Precondition violation - the given 'document' is not managed by this builder!");
		}
	}

	/**
	 * Returns all open (i.e. non-terminated) {@link ChunkDbIndexDocumentData} elements that match the given keyspace
	 * and key.
	 *
	 * @param keyspaceName
	 *            The keyspace to look for. Must not be <code>null</code>.
	 * @param key
	 *            The key to look for. Must not be <code>null</code>.
	 *
	 * @return An unmodifiable view on a set containing all matching documents. May be empty, but never
	 *         <code>null</code>.
	 *
	 * @throws IllegalStateException
	 *             Thrown if this builder is already closed, i.e. {@link #getAllDocumentsAndClose()} has already been
	 *             invoked on it.
	 */
	public Set<ChunkDbIndexDocumentData> getOpenDocuments(final String keyspaceName, final String key) {
		if (this.isClosed) {
			throw new IllegalStateException("This DocumentListBuilder instance is already closed!");
		}
		if (keyspaceName == null) {
			throw new NullPointerException("Precondition violation - argument 'keyspaceName' must not be NULL!");
		}
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		SetMultimap<String, ChunkDbIndexDocumentData> keyToOpenDocs = this.keyspaceToKeyToOpenDocuments
				.get(keyspaceName);
		if (keyToOpenDocs == null) {
			return Collections.emptySet();
		}
		// note: we need to duplicate the set here. The caller will most likely terminate the
		// validity range of one of the returned documents by calling 'terminateDocumentValidity(...)', which
		// will change the internal structure. If the caller is currently iterating over the returned set,
		// this would result in a ConcurrentModificationException.
		return Collections.unmodifiableSet(Sets.newHashSet(keyToOpenDocs.get(key)));
	}

	/**
	 * Returns the list of all documents registered at this builder.
	 *
	 * <p>
	 * <b>/!\ IMPORTANT WARNING /!\</b><br>
	 * For efficiency reasons, this method returns the <b>internal</b> list representation, as copying could potentially
	 * be very expensive! It is <b>imperative</b> that this builder instance is <b>discarded</b> after calling this
	 * method! All methods of this object will throw an {@link IllegalStateException} after calling this method once.
	 *
	 * @return The (mutable) list of documents. May be empty, but never <code>null</code>.
	 *
	 * @throws IllegalStateException
	 *             Thrown if this builder is already closed, i.e. {@link #getAllDocumentsAndClose()} has already been
	 *             invoked on it.
	 */
	public List<ChunkDbIndexDocumentData> getAllDocumentsAndClose() {
		if (this.isClosed) {
			throw new IllegalStateException("This DocumentListBuilder instance is already closed!");
		}
		this.isClosed = true;
		return this.allDocuments;
	}
}