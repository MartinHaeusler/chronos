package org.chronos.chronodb.test.engine.indexing;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Set;

import org.chronos.chronodb.internal.impl.engines.chunkdb.index.ChunkDbIndexDocumentData;
import org.chronos.chronodb.internal.impl.engines.chunkdb.index.DocumentListBuilder;
import org.chronos.common.test.ChronosUnitTest;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class DocumentListBuilderTest extends ChronosUnitTest {

	@Test
	public void canCreateDocumentListBuilder() {
		DocumentListBuilder builder = new DocumentListBuilder();
		assertNotNull(builder);
	}

	@Test
	public void canAddAndCloseDocuments() {
		DocumentListBuilder builder = new DocumentListBuilder();
		ChunkDbIndexDocumentData doc = new ChunkDbIndexDocumentData("idx", "default", "a", "Hello", 1000);
		builder.addDocument(doc);
		Set<ChunkDbIndexDocumentData> openDocuments = builder.getOpenDocuments("default", "a");
		assertEquals(1, openDocuments.size());
		ChunkDbIndexDocumentData doc2 = Iterables.getOnlyElement(openDocuments);
		assertNotNull(doc2);
		assertEquals("idx", doc2.getIndexName());
		assertEquals("default", doc2.getKeyspace());
		assertEquals("a", doc2.getKey());
		assertEquals("Hello", doc2.getIndexedValue());
		assertEquals(1000, doc2.getValidFromTimestamp());
		assertEquals(Long.MAX_VALUE, doc2.getValidToTimestamp());
		builder.terminateDocumentValidity(doc, 3000);
		assertEquals(0, builder.getOpenDocuments("default", "a").size());
	}

	@Test
	public void retrievingIndexWithoutDocumentsProducesEmptySet() {
		DocumentListBuilder builder = new DocumentListBuilder();
		Set<ChunkDbIndexDocumentData> set = builder.getOpenDocuments("test", "any");
		assertNotNull(set);
		assertTrue(set.isEmpty());
	}

	@Test
	public void cannotAccessBuilderAfterClosing() {
		DocumentListBuilder builder = new DocumentListBuilder();
		List<ChunkDbIndexDocumentData> docs = builder.getAllDocumentsAndClose();
		assertNotNull(docs);
		assertTrue(docs.isEmpty());
		try {
			builder.addDocument(new ChunkDbIndexDocumentData("x", "y", "z", "a", 123));
			fail();
		} catch (IllegalStateException expected) {
			// pass
		}
		try {
			builder.terminateDocumentValidity(new ChunkDbIndexDocumentData("x", "y", "z", "a", 1234), 5678);
			fail();
		} catch (IllegalStateException expected) {
			// pass
		}
		try {
			builder.getAllDocumentsAndClose();
			fail();
		} catch (IllegalStateException expected) {
			// pass
		}

	}
}
