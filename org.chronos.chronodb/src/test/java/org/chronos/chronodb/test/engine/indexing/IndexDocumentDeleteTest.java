package org.chronos.chronodb.test.engine.indexing;

import static org.junit.Assert.*;

import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
import org.chronos.chronodb.test.util.model.payload.NamedPayloadNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class IndexDocumentDeleteTest extends AllChronoDBBackendsTest {

	@Test
	public void testDeletedIndexEntryIsNoLongerPresent() {
		ChronoDB db = this.getChronoDB();
		StringIndexer nameIndexer = new NamedPayloadNameIndexer();
		db.getIndexManager().addIndexer("name", nameIndexer);
		db.getIndexManager().reindexAll();

		// generate and insert test data
		NamedPayload np1 = NamedPayload.create1KB("Hello World");
		NamedPayload np2 = NamedPayload.create1KB("Foo Bar");
		NamedPayload np3 = NamedPayload.create1KB("Foo Baz");
		ChronoDBTransaction tx = db.tx();
		tx.put("np1", np1);
		tx.put("np2", np2);
		tx.put("np3", np3);
		tx.commit();
		long afterFirstWrite = tx.getTimestamp();

		// now, delete np2
		tx.remove("np2");
		tx.commit();

		// open a read transaction after the first write
		ChronoDBTransaction tx2 = db.tx(afterFirstWrite);
		// assert that the "hello world" element is there
		long count = tx2.find().inDefaultKeyspace().where("name").isEqualTo("Foo Bar").count();
		assertEquals(1, count);

		// open a read transaction on "now" and assert that it is gone
		ChronoDBTransaction tx3 = db.tx();
		long count2 = tx3.find().inDefaultKeyspace().where("name").isEqualTo("Foo Bar").count();
		assertEquals(0, count2);

	}

	@Test
	public void testRecreationViaSaveOfDeletedEntryWorks() throws Exception {
		ChronoDB db = this.getChronoDB();
		StringIndexer nameIndexer = new NamedPayloadNameIndexer();
		db.getIndexManager().addIndexer("name", nameIndexer);
		db.getIndexManager().reindexAll();

		final long afterFirstWrite;
		{
			// generate and insert test data
			NamedPayload np1 = NamedPayload.create1KB("A");
			NamedPayload np2 = NamedPayload.create1KB("A");
			ChronoDBTransaction tx = db.tx();
			tx.put("np1", np1);
			tx.put("np2", np2);
			tx.commit();
			afterFirstWrite = tx.getTimestamp();
		}

		final long afterSecondWrite;
		{
			// generate and insert test data
			ChronoDBTransaction tx = db.tx();
			tx.remove("np1");
			tx.commit();
			afterSecondWrite = tx.getTimestamp();
		}

		final long afterThirdWrite;
		{
			// generate and insert test data
			ChronoDBTransaction tx = db.tx();
			NamedPayload np1 = NamedPayload.create1KB("A");
			tx.put("np1", np1);
			tx.commit();
			afterThirdWrite = tx.getTimestamp();
		}

		final long afterFourthWrite;
		{
			// generate and insert test data
			ChronoDBTransaction tx = db.tx();
			tx.remove("np1");
			tx.commit();
			afterFourthWrite = tx.getTimestamp();
		}

		// two 'A' after first write
		Set<String> aAfterFirstWrite = db.tx(afterFirstWrite).find().inDefaultKeyspace().where("name")
				.isEqualTo("A")
				.getKeysAsSet().stream().map(QualifiedKey::getKey).collect(Collectors.toSet());
		assertEquals(Sets.newHashSet("np1", "np2"), aAfterFirstWrite);

		// only one 'A' after second write
		Set<String> aAfterSecondWrite = db.tx(afterSecondWrite).find().inDefaultKeyspace().where("name")
				.isEqualTo("A")
				.getKeysAsSet().stream().map(QualifiedKey::getKey).collect(Collectors.toSet());
		assertEquals(Sets.newHashSet("np2"), aAfterSecondWrite);

		// two 'A' after third write
		Set<String> aAfterThirdWrite = db.tx(afterThirdWrite).find().inDefaultKeyspace().where("name")
				.isEqualTo("A")
				.getKeysAsSet().stream().map(QualifiedKey::getKey).collect(Collectors.toSet());
		assertEquals(Sets.newHashSet("np1", "np2"), aAfterThirdWrite);

		// only one 'A' after fourth write
		Set<String> aAfterFourthWrite = db.tx(afterFourthWrite).find().inDefaultKeyspace().where("name")
				.isEqualTo("A")
				.getKeysAsSet().stream().map(QualifiedKey::getKey).collect(Collectors.toSet());
		assertEquals(Sets.newHashSet("np2"), aAfterFourthWrite);

	}

	// private void printIndexState(final ChronoDB db) throws Exception {
	// // state of secondary index
	// ChronoChunk chunk = ((ChunkedChronoDB) db).getChunkManager().getChunkManagerForBranch("master")
	// .getChunkForTimestamp(db.tx().getTimestamp());
	// // fetch the index manager for this chunk
	// ChunkDbIndexManagerBackend indexManagerBackend = ((ChunkDbIndexManager) db.getIndexManager())
	// .getIndexManagerBackend();
	// Field indexManagerField = ChunkDbIndexManagerBackend.class.getDeclaredField("indexChunkManager");
	// indexManagerField.setAccessible(true);
	// IndexChunkManager indexChunkManager = (IndexChunkManager) indexManagerField.get(indexManagerBackend);
	// DocumentBasedChunkIndex chunkIndex = indexChunkManager.getIndexForChunk(chunk);
	// Field indexNameToDocumentField = InMemoryIndexManagerBackend.class.getDeclaredField("indexNameToDocuments");
	// indexNameToDocumentField.setAccessible(true);
	// SetMultimap<String, ChronoIndexDocument> indexNameToDocument = (SetMultimap<String, ChronoIndexDocument>) indexNameToDocumentField
	// .get(chunkIndex);
	//
	// for (ChronoIndexDocument doc : indexNameToDocument.get("name")) {
	// System.out.println(doc);
	// }
	// }
}
