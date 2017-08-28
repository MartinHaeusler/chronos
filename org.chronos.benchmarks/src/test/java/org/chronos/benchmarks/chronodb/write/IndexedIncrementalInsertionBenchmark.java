package org.chronos.benchmarks.chronodb.write;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.chronos.benchmarks.util.BenchmarkUtils;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.utils.Measure;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(PerformanceTest.class)
public class IndexedIncrementalInsertionBenchmark extends AllChronoDBBackendsTest {

	public static final int ENTRIES = 100_000;
	public static final int BATCH_SIZE = 25_000;

	private static final List<String> FIRST_NAMES = Lists.newArrayList("Kareem", "Sunshine", "Dara", "Shelton", "Mercedez", "Fabiola", "Sherryl", "Bronwyn", "Al", "Keren", "Hien", "Colleen", "Burma", "Leonie", "Shizue", "Antonietta", "Sherice", "Lashon", "Ashlee", "Vernia", "Albert", "Mallory", "Vernon", "Demarcus", "Jene", "Ava", "Vanessa", "Dagny", "Darin", "Lorinda");
	private static final List<String> LAST_NAMES = Lists.newArrayList("Stoker", "Garling", "Bechard", "Desjardins", "Clukey", "Dey", "Weisgerber", "Versace", "Albright", "Morquecho", "Volker", "Bench", "Mulcahy", "Shank", "Richarson", "Mcdonald", "Lisa", "Weishaar", "Sheeler", "Caouette", "Gilroy", "Washam", "Olah", "Hoffert", "Presutti", "Chavera", "Hofmann", "Board", "Terranova", "Dahlgren");

	@Test
	public void runTest() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("firstname", new PersonFirstNameIndexer());
		db.getIndexManager().addIndexer("lastname", new PersonLastNameIndexer());
		db.getIndexManager().addIndexer("age", new PersonAgeIndexer());
		db.getIndexManager().addIndexer("id", new PersonIdIndexer());
		ChronoDBTransaction tx = db.tx();
		int commitCount = 0;
		for (int iteration = 0; iteration < ENTRIES; iteration++) {
			Person person = new Person();
			tx.put(person.getId(), person);
			if (iteration > 0 && iteration % BATCH_SIZE == 0) {
				Measure.startTimeMeasure("commit " + commitCount);
				tx.commitIncremental();
				Measure.endTimeMeasure("commit " + commitCount);
				commitCount++;
			}
		}
		Measure.startTimeMeasure("Full Commit");
		tx.commit();
		Measure.endTimeMeasure("Full Commit");
	}

	private static class Person {

		private final String id;
		private final String firstName;
		private final String lastName;
		private final int age;

		public Person() {
			this.id = UUID.randomUUID().toString();
			this.firstName = BenchmarkUtils.getRandomEntryOf(FIRST_NAMES);
			this.lastName = BenchmarkUtils.getRandomEntryOf(LAST_NAMES);
			this.age = BenchmarkUtils.randomBetween(18, 99);
		}

		public String getId() {
			return this.id;
		}

		public String getFirstName() {
			return this.firstName;
		}

		public String getLastName() {
			return this.lastName;
		}

		public int getAge() {
			return this.age;
		}

	}

	private abstract static class PersonIndexer implements StringIndexer {

		@Override
		public boolean canIndex(final Object object) {
			return object != null && object instanceof Person;
		}

	}

	private static class PersonFirstNameIndexer extends PersonIndexer {

		@Override
		public Set<String> getIndexValues(final Object object) {
			return Collections.singleton(((Person) object).getFirstName());
		}

	}

	private static class PersonLastNameIndexer extends PersonIndexer {

		@Override
		public Set<String> getIndexValues(final Object object) {
			return Collections.singleton(((Person) object).getLastName());
		}

	}

	private static class PersonAgeIndexer extends PersonIndexer {

		@Override
		public Set<String> getIndexValues(final Object object) {
			return Collections.singleton("" + ((Person) object).getAge());
		}

	}

	private static class PersonIdIndexer extends PersonIndexer {

		@Override
		public Set<String> getIndexValues(final Object object) {
			return Collections.singleton(((Person) object).getId());
		}

	}
}
