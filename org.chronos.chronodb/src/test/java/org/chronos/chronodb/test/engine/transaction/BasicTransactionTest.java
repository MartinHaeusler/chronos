package org.chronos.chronodb.test.engine.transaction;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.exceptions.ChronoDBTransactionException;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterators;

@Category(IntegrationTest.class)
public class BasicTransactionTest extends AllChronoDBBackendsTest {

	@Test
	public void basicCommitAndGetWorks() {
		ChronoDB chronoDB = this.getChronoDB();
		ChronoDBTransaction tx = chronoDB.tx();
		tx.put("Hello", "World");
		tx.put("Foo", "Bar");
		tx.commit();
		assertEquals("World", tx.get("Hello"));
		assertEquals("Bar", tx.get("Foo"));
	}

	@Test
	public void cantOpenTransactionIntoTheFuture() {
		ChronoDB chronoDB = this.getChronoDB();
		ChronoDBTransaction tx = chronoDB.tx();
		tx.put("Hello", "World");
		tx.put("Foo", "Bar");
		tx.commit();
		try {
			tx = chronoDB.tx(System.currentTimeMillis() + 1000);
			fail("Managed to open a transaction into the future!");
		} catch (ChronoDBTransactionException e) {
			// expected
		}
	}

	@Test
	public void canRemoveKeys() {
		ChronoDB chronoDB = this.getChronoDB();
		ChronoDBTransaction tx = chronoDB.tx();
		tx.put("Hello", "World");
		tx.put("Foo", "Bar");
		tx.commit();
		tx.remove("Hello");
		tx.commit();

		assertEquals(false, tx.exists("Hello"));
		assertEquals(2, Iterators.size(tx.history("Hello")));
	}

	@Test
	public void removalOfKeysAffectsKeySet() {
		ChronoDB chronoDB = this.getChronoDB();
		ChronoDBTransaction tx = chronoDB.tx();
		tx.put("Hello", "World");
		tx.put("Foo", "Bar");
		tx.commit();
		tx.remove("Hello");
		tx.commit();

		assertFalse(tx.exists("Hello"));
		assertEquals(1, tx.keySet().size());
	}

	@Test
	public void removedKeysNoLongerExist() {
		ChronoDB chronoDB = this.getChronoDB();
		ChronoDBTransaction tx = chronoDB.tx();
		tx.put("Hello", "World");
		tx.put("Foo", "Bar");
		tx.commit();
		tx.remove("Hello");
		tx.commit();

		assertTrue(tx.exists("Foo"));
		assertFalse(tx.exists("Hello"));
	}

	@Test
	public void transactionsAreIsolated() {
		ChronoDB chronoDB = this.getChronoDB();

		ChronoDBTransaction tx = chronoDB.tx();
		tx.put("Value1", 1234);
		tx.put("Value2", 1000);
		tx.commit();

		// Transaction A: read-only; used to check isolation level
		ChronoDBTransaction txA = chronoDB.tx();

		// Transaction B: will set Value1 to 47
		ChronoDBTransaction txB = chronoDB.tx();

		// Transaction C: will set Value2 to 2000
		ChronoDBTransaction txC = chronoDB.tx();

		// perform the work in C (while B is open)
		txC.put("Value2", 2000);
		txC.commit();

		// make sure that isolation level of Transaction A is not violated
		assertEquals(1234, (int) txA.get("Value1"));
		assertEquals(1000, (int) txA.get("Value2"));

		// perform work in B (note that we change different keys than in C)
		txB.put("Value1", 47);
		txB.commit();

		// make sure that isolation level of Transaction A is not violated
		assertEquals(1234, (int) txA.get("Value1"));
		assertEquals(1000, (int) txA.get("Value2"));

		// Transaction D: read-only; used to check that commits were successful
		ChronoDBTransaction txD = chronoDB.tx();

		// ensure that D sees the results of B and C
		assertEquals(47, (int) txD.get("Value1"));
		assertEquals(2000, (int) txD.get("Value2"));
	}

	@Test
	public void canRemoveAndPutInSameTransaction() {
		ChronoDB chronoDB = this.getChronoDB();

		ChronoDBTransaction tx1 = chronoDB.tx();
		tx1.put("Hello", "World");
		tx1.put("programming", "Foo", "Bar");
		tx1.commit();

		ChronoDBTransaction tx2 = chronoDB.tx();
		tx2.remove("Hello");
		tx2.remove("programming", "Foo");
		tx2.put("Hello", "ChronoDB");
		tx2.put("programming", "Foo", "Baz");
		tx2.commit();

		ChronoDBTransaction tx3 = chronoDB.tx();
		assertEquals("ChronoDB", tx3.get("Hello"));
		assertEquals("Baz", tx3.get("programming", "Foo"));
	}
}
