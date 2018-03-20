package org.chronos.chronodb.test.conflict;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Set;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.conflict.AtomicConflict;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitConflictException;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronodb.test.util.model.payload.NamedPayload;
import org.chronos.chronodb.test.util.model.payload.NamedPayloadNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class CommitConflictIntegrationTest extends AllChronoDBBackendsTest {

	@Test
	public void commitConflictDetectionWorks() {
		ChronoDB db = this.getChronoDB();
		// create a transaction at an early timestamp
		ChronoDBTransaction tx1 = db.txBuilder().build();
		// create another transaction and write something
		ChronoDBTransaction tx2 = db.tx();
		tx2.put("key", "value");
		tx2.commit();
		// attempt to overwrite with tx1
		tx1.put("key", 123);
		try {
			tx1.commit();
			fail();
		} catch (ChronoDBCommitConflictException expected) {
		}
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY, value = "OVERWRITE_WITH_SOURCE")
	public void overwriteWithSourceStrategyWorks() {
		ChronoDB db = this.getChronoDB();
		// create a transaction at an early timestamp
		ChronoDBTransaction tx1 = db.txBuilder().build();
		// create another transaction and write something
		ChronoDBTransaction tx2 = db.tx();
		tx2.put("key", "value");
		tx1.put("other", "hello");
		tx2.commit();
		// attempt to overwrite with tx1
		tx1.put("key", 123);
		tx1.put("else", "world");
		tx1.commit();
		// we should have 123 in the store now
		assertThat(db.tx().get("key"), is(123));
		assertThat(db.tx().get("other"), is("hello"));
		assertThat(db.tx().get("else"), is("world"));
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY, value = "OVERWRITE_WITH_TARGET")
	public void overwriteWithTargetStrategyWorks() {
		ChronoDB db = this.getChronoDB();
		// create a transaction at an early timestamp
		ChronoDBTransaction tx1 = db.txBuilder().build();
		// create another transaction and write something
		ChronoDBTransaction tx2 = db.tx();
		tx2.put("key", "value");
		tx2.put("other", "hello");
		tx2.commit();
		// attempt to overwrite with tx1
		tx1.put("key", 123);
		tx1.put("else", "world");
		tx1.commit();
		// we should have "value" in the store now
		assertThat(db.tx().get("key"), is("value"));
		assertThat(db.tx().get("other"), is("hello"));
		assertThat(db.tx().get("else"), is("world"));
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10000")
	@InstantiateChronosWith(property = ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY, value = "OVERWRITE_WITH_SOURCE")
	public void indexingWorksWithOverwriteWithSource() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("name", new NamedPayloadNameIndexer());
		// create a transaction at an early timestamp using a different strategy
		ChronoDBTransaction tx1 = db.tx();
		// create another transaction and write something
		ChronoDBTransaction tx2 = db.tx();
		tx2.put("k1", NamedPayload.create1KB("hello"));
		tx2.put("k2", NamedPayload.create1KB("world"));
		tx2.commit();
		// attempt to overwrite with tx1
		tx1.put("k1", NamedPayload.create1KB("foo"));
		tx1.put("k3", NamedPayload.create1KB("bar"));
		tx1.commit();

		// check the state of the store
		assertThat(((NamedPayload) db.tx().get("k1")).getName(), is("foo"));
		assertThat(((NamedPayload) db.tx().get("k2")).getName(), is("world"));
		assertThat(((NamedPayload) db.tx().get("k3")).getName(), is("bar"));

		// check the state of the secondary index
		{
			Set<QualifiedKey> qKeys = db.tx().find().inDefaultKeyspace().where("name").isEqualTo("foo").getKeysAsSet();
			assertThat(qKeys.size(), is(1));
			assertThat(qKeys, contains(QualifiedKey.createInDefaultKeyspace("k1")));
		}
		{
			Set<QualifiedKey> qKeys = db.tx().find().inDefaultKeyspace().where("name").isEqualTo("world")
					.getKeysAsSet();
			assertThat(qKeys.size(), is(1));
			assertThat(qKeys, contains(QualifiedKey.createInDefaultKeyspace("k2")));
		}
		{
			Set<QualifiedKey> qKeys = db.tx().find().inDefaultKeyspace().where("name").isEqualTo("bar").getKeysAsSet();
			assertThat(qKeys.size(), is(1));
			assertThat(qKeys, contains(QualifiedKey.createInDefaultKeyspace("k3")));
		}
		{
			Set<QualifiedKey> qKeys = db.tx().find().inDefaultKeyspace().where("name").isEqualTo("hello")
					.getKeysAsSet();
			assertThat(qKeys.size(), is(0));
		}
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10000")
	@InstantiateChronosWith(property = ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY, value = "OVERWRITE_WITH_TARGET")
	public void indexingWorksWithOverwriteWithTarget() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("name", new NamedPayloadNameIndexer());
		// create a transaction at an early timestamp using a different strategy
		ChronoDBTransaction tx1 = db.tx();
		// create another transaction and write something
		ChronoDBTransaction tx2 = db.tx();
		tx2.put("k1", NamedPayload.create1KB("hello"));
		tx2.put("k2", NamedPayload.create1KB("world"));
		tx2.commit();
		assertTrue(db.tx().getTimestamp() > 0);
		// attempt to overwrite with tx1
		tx1.put("k1", NamedPayload.create1KB("foo"));
		tx1.put("k3", NamedPayload.create1KB("bar"));
		tx1.commit();

		// check the state of the store
		assertThat(((NamedPayload) db.tx().get("k1")).getName(), is("hello"));
		assertThat(((NamedPayload) db.tx().get("k2")).getName(), is("world"));
		assertThat(((NamedPayload) db.tx().get("k3")).getName(), is("bar"));

		// check the state of the secondary index
		{
			Set<QualifiedKey> qKeys = db.tx().find().inDefaultKeyspace().where("name").isEqualTo("hello")
					.getKeysAsSet();
			assertThat(qKeys.size(), is(1));
			assertThat(qKeys, contains(QualifiedKey.createInDefaultKeyspace("k1")));
		}
		{
			Set<QualifiedKey> qKeys = db.tx().find().inDefaultKeyspace().where("name").isEqualTo("world")
					.getKeysAsSet();
			assertThat(qKeys.size(), is(1));
			assertThat(qKeys, contains(QualifiedKey.createInDefaultKeyspace("k2")));
		}
		{
			Set<QualifiedKey> qKeys = db.tx().find().inDefaultKeyspace().where("name").isEqualTo("bar").getKeysAsSet();
			assertThat(qKeys.size(), is(1));
			assertThat(qKeys, contains(QualifiedKey.createInDefaultKeyspace("k3")));
		}
	}

	@Test
	public void canOverrideDefaultResolutionStrategyInTransaction() {
		ChronoDB db = this.getChronoDB();
		// by default, we should have DO_NOT_MERGE as our strategy
		assertThat(db.getConfiguration().getConflictResolutionStrategy(), is(ConflictResolutionStrategy.DO_NOT_MERGE));
		// create a transaction at an early timestamp using a different strategy
		ChronoDBTransaction tx1 = db.txBuilder()
				.withConflictResolutionStrategy(ConflictResolutionStrategy.OVERWRITE_WITH_SOURCE).build();
		// create another transaction and write something
		ChronoDBTransaction tx2 = db.tx();
		tx2.put("key", "value");
		tx2.put("other", "hello");
		tx2.commit();
		// attempt to overwrite with tx1
		tx1.put("key", 123);
		tx1.put("else", "world");
		tx1.commit();
		// we should have 123 in the store now (because we have chosen "override with source")
		assertThat(db.tx().get("key"), is(123));
		assertThat(db.tx().get("other"), is("hello"));
		assertThat(db.tx().get("else"), is("world"));
	}

	@Test
	public void canUseCustomConflictResolverClass() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx1 = db.tx();
		ChronoDBTransaction tx2 = db.txBuilder()
				.withConflictResolutionStrategy(new TakeHigherNumericValueConflictResolver()).build();
		tx1.put("a", 3);
		tx1.put("b", 25);
		tx1.put("c", 36);
		tx1.commit();
		tx2.put("a", 15);
		tx2.put("b", 10);
		tx2.put("d", 1);
		tx2.commit();

		// assert that the higher value survived in case of conflicts
		assertThat(db.tx().get("a"), is(15));
		assertThat(db.tx().get("b"), is(25));
		assertThat(db.tx().get("c"), is(36));
		assertThat(db.tx().get("d"), is(1));
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10000")
	@InstantiateChronosWith(property = ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY, value = "OVERWRITE_WITH_SOURCE")
	public void canConflictResolveDeletions() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("name", new NamedPayloadNameIndexer());
		ChronoDBTransaction tx0 = db.tx();
		tx0.put("k1", "hello");
		tx0.commit();

		// create a transaction at an early timestamp using a different strategy
		ChronoDBTransaction tx1 = db.tx();
		// create another transaction and write something
		ChronoDBTransaction tx2 = db.tx();
		tx2.put("k1", NamedPayload.create1KB("hello"));
		tx2.put("k2", NamedPayload.create1KB("world"));
		tx2.commit();
		// attempt to overwrite with tx1
		tx1.remove("k1");
		tx1.put("k3", NamedPayload.create1KB("bar"));
		tx1.commit();

		// check the state of the store
		assertThat(db.tx().get("k1"), nullValue());
		assertThat(((NamedPayload) db.tx().get("k2")).getName(), is("world"));
		assertThat(((NamedPayload) db.tx().get("k3")).getName(), is("bar"));

		Set<QualifiedKey> qKeys = db.tx().find().inDefaultKeyspace().where("name").isEqualTo("hello").getKeysAsSet();
		assertThat(qKeys.size(), is(0));
	}

	// =================================================================================================================
	// HELPER CLASSES
	// =================================================================================================================

	public static class TakeHigherNumericValueConflictResolver implements ConflictResolutionStrategy {

		@Override
		public Object resolve(final AtomicConflict conflict) {
			Object sourceValue = conflict.getSourceValue();
			Object targetValue = conflict.getTargetValue();
			if (sourceValue == null) {
				return targetValue;
			}
			if (targetValue == null) {
				return sourceValue;
			}
			if (sourceValue instanceof Number && targetValue instanceof Number) {
				if (((Number) sourceValue).doubleValue() > ((Number) targetValue).doubleValue()) {
					return sourceValue;
				} else {
					return targetValue;
				}
			} else {
				return sourceValue;
			}
		}

	}
}
