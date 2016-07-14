package org.chronos.chronodb.test.engine.safetyfeatures;

import static org.junit.Assert.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.exceptions.BlindOverwriteException;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class BlindOverwriteProtectionTest extends AllChronoDBBackendsTest {

	@Test
	public void blindOverwriteProtectionWorks() {
		ChronoDB db = this.getChronoDB();
		// create a transaction at an early timestamp
		ChronoDBTransaction tx1 = db.txBuilder().withBlindOverwriteProtection(true).build();
		// create another transaction and write something
		ChronoDBTransaction tx2 = db.tx();
		tx2.put("key", "value");
		tx2.commit();
		// attempt to overwrite with tx1
		tx1.put("key", 123);
		try {
			tx1.commit();
			fail();
		} catch (BlindOverwriteException expected) {
		}

	}

}
