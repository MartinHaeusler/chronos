package org.chronos.chronodb.test.engine.query;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
import org.chronos.chronodb.test.util.model.payload.NamedPayloadNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class BasicQueryTest extends AllChronoDBBackendsTest {

	@Test
	public void basicQueryingWorks() {
		ChronoDB db = this.getChronoDB();
		// set up the "name" index
		ChronoIndexer nameIndexer = new NamedPayloadNameIndexer();
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

		Iterator<QualifiedKey> queryResultIterator = tx.find().inDefaultKeyspace().where("name").contains("Foo").and()
				.not().where("name").contains("Bar").getKeys();
		Set<QualifiedKey> queryResultSet = Sets.newHashSet(queryResultIterator);
		assertEquals(1, queryResultSet.size());
		QualifiedKey singleResult = Iterables.getOnlyElement(queryResultSet);
		assertEquals("np3", singleResult.getKey());
	}

	@Test
	public void queryResultSetMethodsWork() {
		ChronoDB db = this.getChronoDB();
		// set up the "name" index
		ChronoIndexer nameIndexer = new NamedPayloadNameIndexer();
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

		ChronoDBQuery query = tx.find().inDefaultKeyspace().where("name").contains("Foo").and().not().where("name")
				.contains("Bar").toQuery();

		{ // using queryBuilder.getKeys()
			Iterator<QualifiedKey> keys = tx.find(query).getKeys();
			Set<String> keySet = Sets.newHashSet(keys).stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
			assertTrue(keySet.contains("np3"));
			assertEquals(1, keySet.size());
		}

		{ // using queryBuilder.values()
			Iterator<Object> values = tx.find(query).getValues();
			Set<Object> valueSet = Sets.newHashSet(values);
			assertEquals(1, valueSet.size());
			assertEquals("Foo Baz", ((NamedPayload) Iterables.getOnlyElement(valueSet)).getName());
		}

		{ // using queryBuilder.getResult()
			Iterator<Entry<String, Object>> result = tx.find(query).getResult();
			Set<Entry<String, Object>> resultSet = Sets.newHashSet(result);
			assertEquals(1, resultSet.size());
			Entry<String, Object> entry = Iterables.getOnlyElement(resultSet);
			assertEquals("np3", entry.getKey());
			assertEquals("Foo Baz", ((NamedPayload) entry.getValue()).getName());
		}

		{ // using queryBuilder.getQualifiedResult()
			Iterator<Entry<QualifiedKey, Object>> qualifiedResult = tx.find(query).getQualifiedResult();
			Entry<QualifiedKey, Object> entry = Iterators.getOnlyElement(qualifiedResult);
			QualifiedKey qKey = entry.getKey();
			String key = qKey.getKey();
			String keyspace = qKey.getKeyspace();
			NamedPayload value = (NamedPayload) entry.getValue();
			assertEquals("np3", key);
			assertEquals(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, keyspace);
			assertEquals("Foo Baz", value.getName());
		}
	}

	@Test
	public void doubleNegationEliminationWorks() {
		ChronoDB db = this.getChronoDB();
		// set up the "name" index
		ChronoIndexer nameIndexer = new NamedPayloadNameIndexer();
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

		Iterator<QualifiedKey> keys = tx.find().inDefaultKeyspace().not().not().not().where("name")
				.isNotEqualTo("Hello World").getKeys();
		Set<String> keySet = Sets.newHashSet(keys).stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
		assertEquals(1, keySet.size());
		assertTrue(keySet.contains("np1"));
	}

}
