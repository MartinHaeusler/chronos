package org.chronos.chronodb.test.engine.keyspace;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;

@Category(IntegrationTest.class)
public class KeyspaceTest extends AllChronoDBBackendsTest {

	@Test
	public void creatingAndReadingAKeyspaceWorks() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();

		// initially, only the default keyspace exists
		assertEquals(1, tx.keyspaces().size());
		assertEquals(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, Iterables.getOnlyElement(tx.keyspaces()));

		// put something into a variety of keyspaces (which dynamically creates them on the fly)
		tx.put("Programs", "Eclipse", "IBM");
		tx.put("Programs", "VisualStudio", "Microsoft");
		tx.put("OperatingSystems", "Windows 8", "Microsoft");
		tx.put("OperatingSystems", "Windows XP", "Microsoft");
		tx.put("OperatingSystems", "Ubuntu", "Canonical");
		tx.put("OperatingSystems", "OSX", "Apple");
		tx.commit();

		// the default keyspace should be empty (we added nothing to it)
		assertTrue(tx.keySet().isEmpty());
		// the "Programs" keyspace should contain 2 entries
		assertEquals(2, tx.keySet("Programs").size());
		// the "OperatingSystems" keyspace should contain 4 entries
		assertEquals(4, tx.keySet("OperatingSystems").size());

		// using the default keyspace, none of our keys works
		assertNull(tx.get("Eclipse"));
		assertNull(tx.get("VisualStudio"));
		assertNull(tx.get("Windows 8"));
		assertNull(tx.get("Windows XP"));
		assertNull(tx.get("Ubuntu"));
		assertNull(tx.get("OSX"));
		assertEquals(false, tx.exists("Eclipse"));
		assertEquals(false, tx.exists("VisualStudio"));
		assertEquals(false, tx.exists("Windows 8"));
		assertEquals(false, tx.exists("Windows XP"));
		assertEquals(false, tx.exists("Ubuntu"));
		assertEquals(false, tx.exists("OSX"));

		// assert that the "Programs" keyspace contains the correct entries
		assertEquals("IBM", tx.get("Programs", "Eclipse"));
		assertEquals("Microsoft", tx.get("Programs", "VisualStudio"));
		assertNull(tx.get("Programs", "Windows 8"));
		assertNull(tx.get("Programs", "Windows XP"));
		assertNull(tx.get("Programs", "Ubuntu"));
		assertNull(tx.get("Programs", "OSX"));
		assertEquals(true, tx.exists("Programs", "Eclipse"));
		assertEquals(true, tx.exists("Programs", "VisualStudio"));
		assertEquals(false, tx.exists("Programs", "Windows 8"));
		assertEquals(false, tx.exists("Programs", "Windows XP"));
		assertEquals(false, tx.exists("Programs", "Ubuntu"));
		assertEquals(false, tx.exists("Programs", "OSX"));

		// assert that the "OperatingSystems" keyspace contains the correct entries
		assertNull(tx.get("OperatingSystems", "Eclipse"));
		assertNull(tx.get("OperatingSystems", "VisualStudio"));
		assertEquals("Microsoft", tx.get("OperatingSystems", "Windows 8"));
		assertEquals("Microsoft", tx.get("OperatingSystems", "Windows XP"));
		assertEquals("Canonical", tx.get("OperatingSystems", "Ubuntu"));
		assertEquals("Apple", tx.get("OperatingSystems", "OSX"));
		assertEquals(false, tx.exists("OperatingSystems", "Eclipse"));
		assertEquals(false, tx.exists("OperatingSystems", "VisualStudio"));
		assertEquals(true, tx.exists("OperatingSystems", "Windows 8"));
		assertEquals(true, tx.exists("OperatingSystems", "Windows XP"));
		assertEquals(true, tx.exists("OperatingSystems", "Ubuntu"));
		assertEquals(true, tx.exists("OperatingSystems", "OSX"));
	}

}
