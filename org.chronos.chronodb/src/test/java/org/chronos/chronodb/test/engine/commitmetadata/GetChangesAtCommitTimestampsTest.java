package org.chronos.chronodb.test.engine.commitmetadata;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class GetChangesAtCommitTimestampsTest extends AllChronoDBBackendsTest {

	@Test
	public void canRetrieveChangesFromDefaultKeyspace() {
		ChronoDB db = this.getChronoDB();
		long timestamp1;
		long timestamp2;
		long timestamp3;
		{ // set up
			ChronoDBTransaction tx = db.tx();
			tx.put("pi", 3.1415);
			tx.put("hello", "world");
			tx.put("foo", "bar");
			tx.commit();
			timestamp1 = tx.getTimestamp();
			tx.put("foo", "baz");
			tx.put("hello", "john");
			tx.commit();
			timestamp2 = tx.getTimestamp();
			tx.put("new", "value");
			tx.commit();
			timestamp3 = tx.getTimestamp();
		}
		{// check timestamp1
			Iterator<String> changedKeys = db.tx().getChangedKeysAtCommit(timestamp1);
			Set<String> keys = Sets.newHashSet(changedKeys);
			assertEquals(Sets.newHashSet("pi", "foo", "hello"), keys);
		}
		{ // check timestamp2
			Iterator<String> changedKeys = db.tx().getChangedKeysAtCommit(timestamp2);
			Set<String> keys = Sets.newHashSet(changedKeys);
			assertEquals(Sets.newHashSet("foo", "hello"), keys);
		}
		{ // check timestamp3
			Iterator<String> changedKeys = db.tx().getChangedKeysAtCommit(timestamp3);
			Set<String> keys = Sets.newHashSet(changedKeys);
			assertEquals(Sets.newHashSet("new"), keys);
		}
	}

	@Test
	public void canRetrieveChangesFromCustomKeyspace() {
		ChronoDB db = this.getChronoDB();
		long timestamp1;
		long timestamp2;
		long timestamp3;
		{ // set up
			ChronoDBTransaction tx = db.tx();
			tx.put("math", "pi", 3.1415);
			tx.put("greeting", "hello", "world");
			tx.put("nonsense", "foo", "bar");
			tx.commit();
			timestamp1 = tx.getTimestamp();
			tx.put("nonsense", "foo", "baz");
			tx.put("greeting", "hello", "john");
			tx.commit();
			timestamp2 = tx.getTimestamp();
			tx.put("nonsense", "new", "value");
			tx.commit();
			timestamp3 = tx.getTimestamp();
		}
		{// check timestamp1 (math)
			Iterator<String> changedKeys = db.tx().getChangedKeysAtCommit(timestamp1, "math");
			Set<String> keys = Sets.newHashSet(changedKeys);
			assertEquals(Sets.newHashSet("pi"), keys);
		}
		{// check timestamp1 (greeting)
			Iterator<String> changedKeys = db.tx().getChangedKeysAtCommit(timestamp1, "greeting");
			Set<String> keys = Sets.newHashSet(changedKeys);
			assertEquals(Sets.newHashSet("hello"), keys);
		}
		{// check timestamp1 (nonsense)
			Iterator<String> changedKeys = db.tx().getChangedKeysAtCommit(timestamp1, "nonsense");
			Set<String> keys = Sets.newHashSet(changedKeys);
			assertEquals(Sets.newHashSet("foo"), keys);
		}
		{ // check timestamp2 (math)
			Iterator<String> changedKeys = db.tx().getChangedKeysAtCommit(timestamp2, "math");
			Set<String> keys = Sets.newHashSet(changedKeys);
			assertEquals(Collections.emptySet(), keys);
		}
		{ // check timestamp2 (greeting)
			Iterator<String> changedKeys = db.tx().getChangedKeysAtCommit(timestamp2, "greeting");
			Set<String> keys = Sets.newHashSet(changedKeys);
			assertEquals(Sets.newHashSet("hello"), keys);
		}
		{ // check timestamp2 (nonsense)
			Iterator<String> changedKeys = db.tx().getChangedKeysAtCommit(timestamp2, "nonsense");
			Set<String> keys = Sets.newHashSet(changedKeys);
			assertEquals(Sets.newHashSet("foo"), keys);
		}

		{ // check timestamp3 (math)
			Iterator<String> changedKeys = db.tx().getChangedKeysAtCommit(timestamp3, "math");
			Set<String> keys = Sets.newHashSet(changedKeys);
			assertEquals(Collections.emptySet(), keys);
		}
		{ // check timestamp3 (greeting)
			Iterator<String> changedKeys = db.tx().getChangedKeysAtCommit(timestamp3, "greeting");
			Set<String> keys = Sets.newHashSet(changedKeys);
			assertEquals(Collections.emptySet(), keys);
		}
		{ // check timestamp3 (nonsense)
			Iterator<String> changedKeys = db.tx().getChangedKeysAtCommit(timestamp3, "nonsense");
			Set<String> keys = Sets.newHashSet(changedKeys);
			assertEquals(Sets.newHashSet("new"), keys);
		}
	}

	@Test
	public void canRetrieveChangesAtCommitFromBranch() {
		ChronoDB db = this.getChronoDB();
		{ // set up master
			ChronoDBTransaction tx = db.tx();
			tx.put("pi", 3.1415);
			tx.put("hello", "world");
			tx.put("foo", "bar");
			tx.commit();
		}
		{ // set up branch
			db.getBranchManager().createBranch("test");
			ChronoDBTransaction tx = db.tx("test");
			tx.put("hello", "john");
			tx.put("new", "value");
			tx.commit();
		}
		long masterHead = db.tx().getTimestamp();
		long testHead = db.tx("test").getTimestamp();
		// this is the standard master branch query
		assertEquals(Sets.newHashSet("pi", "hello", "foo"),
				Sets.newHashSet(db.tx().getChangedKeysAtCommit(masterHead)));
		// try to fetch the contents of a commit from the branch
		assertEquals(Sets.newHashSet("hello", "new"), Sets.newHashSet(db.tx("test").getChangedKeysAtCommit(testHead)));
		// try to fetch the contents of a commit on a branch that was commited before the branching timestamp
		assertEquals(Sets.newHashSet("pi", "hello", "foo"),
				Sets.newHashSet(db.tx("test").getChangedKeysAtCommit(masterHead)));
	}
}
