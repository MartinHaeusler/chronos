package org.chronos.chronodb.test.engine.indexing;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.exceptions.InvalidIndexAccessException;
import org.chronos.chronodb.api.exceptions.UnknownIndexException;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.util.ReflectiveDoubleIndexer;
import org.chronos.chronodb.test.util.ReflectiveLongIndexer;
import org.chronos.chronodb.test.util.ReflectiveStringIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class NumericIndexingTest extends AllChronoDBBackendsTest {

	// =================================================================================================================
	// LONG INDEX TESTS
	// =================================================================================================================

	@Test
	public void canCreateLongIndex() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("value", new ReflectiveLongIndexer(LongBean.class, "value"));
		assertEquals(1, db.getIndexManager().getIndexers().size());
		assertEquals(1, db.getIndexManager().getLongIndexers().size());
	}

	@Test
	public void canExecuteBasicQueryOnLongIndex() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("value", new ReflectiveLongIndexer(LongBean.class, "value"));
		db.getIndexManager().reindexAll();
		{ // insert some data
			ChronoDBTransaction tx = db.tx();
			tx.put("a", new LongBean(34));
			tx.put("b", new LongBean(27));
			tx.commit();

			tx.put("c", new LongBean(13));
			tx.commit();
		}
		// query the data
		assertKeysEqual("a", "b", db.tx().find().inDefaultKeyspace().where("value").isGreaterThan(20));
		assertKeysEqual("a", db.tx().find().inDefaultKeyspace().where("value").isGreaterThan(27));
		assertKeysEqual("a", "b", db.tx().find().inDefaultKeyspace().where("value").isGreaterThanOrEqualTo(27));
		assertKeysEqual("c", db.tx().find().inDefaultKeyspace().where("value").isEqualTo(13));
		assertKeysEqual("c", db.tx().find().inDefaultKeyspace().where("value").isLessThan(27));
		assertKeysEqual("b", "c", db.tx().find().inDefaultKeyspace().where("value").isLessThanOrEqualTo(27));
	}

	// =================================================================================================================
	// DOUBLE INDEX TESTS
	// =================================================================================================================

	@Test
	public void canCreateDoubleIndex() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("value", new ReflectiveDoubleIndexer(DoubleBean.class, "value"));
		assertEquals(1, db.getIndexManager().getIndexers().size());
		assertEquals(1, db.getIndexManager().getDoubleIndexers().size());
	}

	@Test
	public void canExecuteBasicQueryOnDoubleIndex() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("value", new ReflectiveDoubleIndexer(DoubleBean.class, "value"));
		db.getIndexManager().reindexAll();
		{ // insert some data
			ChronoDBTransaction tx = db.tx();
			tx.put("a", new DoubleBean(34.1));
			tx.put("b", new DoubleBean(27.8));
			tx.commit();

			tx.put("c", new DoubleBean(13.4));
			tx.commit();
		}
		// query the data
		assertKeysEqual("a", "b", db.tx().find().inDefaultKeyspace().where("value").isGreaterThan(20.4));
		assertKeysEqual("a", db.tx().find().inDefaultKeyspace().where("value").isGreaterThan(27.8));
		assertKeysEqual("a", "b", db.tx().find().inDefaultKeyspace().where("value").isGreaterThanOrEqualTo(27.8));
		assertKeysEqual("c", db.tx().find().inDefaultKeyspace().where("value").isEqualTo(13.4, 0.01));
		assertKeysEqual("c", db.tx().find().inDefaultKeyspace().where("value").isLessThan(27.8));
		assertKeysEqual("b", "c", db.tx().find().inDefaultKeyspace().where("value").isLessThanOrEqualTo(27.8));
	}

	// =================================================================================================================
	// NEGATIVE TESTS
	// =================================================================================================================

	@Test
	public void cannotSearchForDoubleValueInLongIndex() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("value", new ReflectiveLongIndexer(LongBean.class, "value"));
		db.getIndexManager().reindexAll();
		{ // insert some data
			ChronoDBTransaction tx = db.tx();
			tx.put("a", new LongBean(34));
			tx.put("b", new LongBean(27));
			tx.commit();

			tx.put("c", new LongBean(13));
			tx.commit();
		}
		try {
			db.tx().find().inDefaultKeyspace().where("value").isEqualTo(34.0, 0.001).getKeysAsSet();
			fail("Managed to access index with query of wrong type!");
		} catch (InvalidIndexAccessException expected) {
			// pass
		}
	}

	@Test
	public void cannotSearchForStringValueInLongIndex() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("value", new ReflectiveLongIndexer(LongBean.class, "value"));
		db.getIndexManager().reindexAll();
		{ // insert some data
			ChronoDBTransaction tx = db.tx();
			tx.put("a", new LongBean(34));
			tx.put("b", new LongBean(27));
			tx.commit();

			tx.put("c", new LongBean(13));
			tx.commit();
		}
		try {
			db.tx().find().inDefaultKeyspace().where("value").isEqualTo("34").getKeysAsSet();
			fail("Managed to access index with query of wrong type!");
		} catch (InvalidIndexAccessException expected) {
			// pass
		}
	}

	@Test
	public void cannotSearchForLongValueInDoubleIndex() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("value", new ReflectiveDoubleIndexer(DoubleBean.class, "value"));
		db.getIndexManager().reindexAll();
		{ // insert some data
			ChronoDBTransaction tx = db.tx();
			tx.put("a", new DoubleBean(34.5));
			tx.put("b", new DoubleBean(27.8));
			tx.commit();

			tx.put("c", new DoubleBean(13.4));
			tx.commit();
		}
		try {
			db.tx().find().inDefaultKeyspace().where("value").isEqualTo(34).getKeysAsSet();
			fail("Managed to access index with query of wrong type!");
		} catch (InvalidIndexAccessException expected) {
			// pass
		}
	}

	@Test
	public void cannotSearchForStringValueInDoubleIndex() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("value", new ReflectiveDoubleIndexer(DoubleBean.class, "value"));
		db.getIndexManager().reindexAll();
		{ // insert some data
			ChronoDBTransaction tx = db.tx();
			tx.put("a", new DoubleBean(34.5));
			tx.put("b", new DoubleBean(27.8));
			tx.commit();

			tx.put("c", new DoubleBean(13.4));
			tx.commit();
		}
		try {
			db.tx().find().inDefaultKeyspace().where("value").isEqualTo("34.0").getKeysAsSet();
			fail("Managed to access index with query of wrong type!");
		} catch (InvalidIndexAccessException expected) {
			// pass
		}
	}

	@Test
	public void cannotSearchForLongValueInStringIndex() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("value", new ReflectiveStringIndexer(StringBean.class, "value"));
		db.getIndexManager().reindexAll();
		{ // insert some data
			ChronoDBTransaction tx = db.tx();
			tx.put("a", new StringBean("34.5"));
			tx.put("b", new StringBean("27.8"));
			tx.commit();

			tx.put("c", new StringBean("13.4"));
			tx.commit();
		}
		try {
			db.tx().find().inDefaultKeyspace().where("value").isEqualTo(34).getKeysAsSet();
			fail("Managed to access index with query of wrong type!");
		} catch (InvalidIndexAccessException expected) {
			// pass
		}
	}

	@Test
	public void cannotSearchForDoubleValueInStringIndex() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("value", new ReflectiveStringIndexer(StringBean.class, "value"));
		db.getIndexManager().reindexAll();
		{ // insert some data
			ChronoDBTransaction tx = db.tx();
			tx.put("a", new StringBean("34.5"));
			tx.put("b", new StringBean("27.8"));
			tx.commit();

			tx.put("c", new StringBean("13.4"));
			tx.commit();
		}
		try {
			db.tx().find().inDefaultKeyspace().where("value").isEqualTo(34.5, 0.001).getKeysAsSet();
			fail("Managed to access index with query of wrong type!");
		} catch (InvalidIndexAccessException expected) {
			// pass
		}
	}

	@Test
	public void cannotQueryNonExistingIndex() {
		ChronoDB db = this.getChronoDB();
		try {
			db.tx().find().inDefaultKeyspace().where("hello").isEqualTo("world").getKeysAsSet();
			fail("Managed to access non-existent index!");
		} catch (UnknownIndexException expected) {
			// pass
		}
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	@SuppressWarnings("unused")
	private static class TestBean<T> {

		private T value;

		protected TestBean() {
		}

		protected TestBean(final T value) {
			checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
			this.value = value;
		}

		public T getValue() {
			return this.value;
		}
	}

	@SuppressWarnings("unused")
	private static class LongBean extends TestBean<Long> {

		protected LongBean() {
		}

		public LongBean(final long value) {
			super(value);
		}

	}

	@SuppressWarnings("unused")
	private static class DoubleBean extends TestBean<Double> {

		protected DoubleBean() {
		}

		public DoubleBean(final double value) {
			super(value);
		}

	}

	@SuppressWarnings("unused")
	private static class StringBean extends TestBean<String> {

		protected StringBean() {
		}

		public StringBean(final String value) {
			super(value);
		}
	}

}
