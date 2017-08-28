package org.chronos.chronodb.test.engine.dbdump;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.time.DayOfWeek;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.DumpOption;
import org.chronos.chronodb.api.dump.ChronoConverter;
import org.chronos.chronodb.api.dump.ChronoDBDumpFormat;
import org.chronos.chronodb.api.dump.annotations.ChronosExternalizable;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.internal.util.ChronosFileUtils;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@Category(IntegrationTest.class)
public class DBDumpTest extends AllChronoDBBackendsTest {

	// =================================================================================================================
	// TEST METHODS
	// =================================================================================================================

	@Test
	public void canCreateDumpFileIfNotExists() {
		ChronoDB db = this.getChronoDB();
		{ // insert test data
			ChronoDBTransaction tx = db.tx();
			tx.put("p1", new Person("John", "Doe"));
			tx.put("p2", new Person("Jane", "Doe"));
			tx.commit();
		}
		// create the file
		File dumpFile = this.createTestFile("Test.chronodump");
		// delete the file (we only want the file "pointer")
		dumpFile.delete();
		// write to output
		db.writeDump(dumpFile,
				// alias person class with a shorter name
				DumpOption.aliasHint(PersonDump.class, "person"),
				// use the default converter for persons
				DumpOption.defaultConverter(Person.class, new PersonDefaultConverter())
		// end of dump command
		);
		// assert that the output file exists
		assertTrue(dumpFile.exists());
		// delete it again
		dumpFile.delete();
	}

	@Test
	public void canLoadDumpWithInMemoryDB() {
		ChronoDB db = this.getChronoDB();

		// fill the database with some data
		ChronoDBTransaction tx = db.tx();
		tx.put("first", 123);
		tx.put("MyKeyspace", "second", 456);
		tx.commit();
		long writeTimestamp1 = tx.getTimestamp();

		this.sleep(5);

		// create a branch
		db.getBranchManager().createBranch("MyBranch");

		this.sleep(5);

		// insert some data into the branch
		tx = db.tx("MyBranch");
		tx.put("Math", "Pi", 31415);
		tx.commit();
		long writeTimestamp2 = tx.getTimestamp();

		this.sleep(5);

		// create a sub-branch
		db.getBranchManager().createBranch("MyBranch", "MySubBranch");

		this.sleep(5);

		// commit something in the master branch
		tx = db.tx();
		tx.put("MyAwesomeKeyspace", "third", 789);
		tx.commit();

		// create the temporary file
		File testDumpFile = this.createTestFile("Test.chronodump");

		// create the dump
		db.writeDump(testDumpFile);

		String fileContents;
		try {
			fileContents = FileUtils.readFileToString(testDumpFile);
			System.out.println(fileContents);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// deserialize the dump
		ChronoDB db2 = ChronoDB.FACTORY.create().inMemoryDatabase().build();
		db2.readDump(testDumpFile);

		ChronoDBTransaction txAfter1 = db2.tx(writeTimestamp1);
		assertEquals(Sets.newHashSet("default", "MyKeyspace"), txAfter1.keyspaces());
		assertEquals(123, (int) txAfter1.get("first"));
		assertEquals(456, (int) txAfter1.get("MyKeyspace", "second"));
		assertNull(db2.tx("MyBranch", writeTimestamp1).get("Math", "Pi"));

		ChronoDBTransaction txAfter2 = db2.tx("MyBranch", writeTimestamp2);
		assertEquals(Sets.newHashSet("default", "MyKeyspace"), txAfter1.keyspaces());
		assertEquals(123, (int) txAfter2.get("first"));
		assertEquals(456, (int) txAfter2.get("MyKeyspace", "second"));
		assertEquals(31415, (int) txAfter2.get("Math", "Pi"));
		assertEquals(Sets.newHashSet("default", "MyKeyspace", "Math"), txAfter2.keyspaces());

		// check that the "MySubBranch" exists
		assertTrue(db2.getBranchManager().existsBranch("MySubBranch"));
		assertNotNull(db2.tx("MySubBranch"));
		assertEquals("MyBranch", db2.getBranchManager().getBranch("MySubBranch").getOrigin().getName());
	}

	@Test
	public void canExportAndImportDumpOnSameDBType() {
		ChronoDB db = this.getChronoDB();

		// fill the database with some data
		ChronoDBTransaction tx = db.tx();
		tx.put("first", 123);
		tx.put("MyKeyspace", "second", 456);
		tx.commit();

		this.sleep(5);

		// create a branch
		db.getBranchManager().createBranch("MyBranch");

		this.sleep(5);

		// insert some data into the branch
		tx = db.tx("MyBranch");
		tx.put("Math", "Pi", 31415);
		tx.commit();

		this.sleep(5);

		// create a sub-branch
		db.getBranchManager().createBranch("MyBranch", "MySubBranch");

		this.sleep(5);

		// commit something in the master branch
		tx = db.tx();
		tx.put("MyAwesomeKeyspace", "third", 789);
		tx.commit();

		// create the temporary file
		File testDumpFile = this.createTestFile("Test.chronodump");

		// create the dump
		db.writeDump(testDumpFile);

		// clear the database
		db = this.reinstantiateDB();
		db.readDump(testDumpFile);

		// assert that each branch contains the correct information
		tx = db.tx("MySubBranch");
		// Keyspaces in 'MySubBranch': default, MyKeyspace, Math
		assertEquals(3, tx.keyspaces().size());
		assertEquals(31415, (int) tx.get("Math", "Pi"));
		assertEquals(456, (int) tx.get("MyKeyspace", "second"));
		assertEquals(123, (int) tx.get("first"));
		assertEquals(false, tx.keyspaces().contains("MyAwesomeKeyspace"));

		tx = db.tx();
		// Keyspaces in 'Master': default, MyKeyspace, MyAwesomeKeyspace
		assertEquals(3, tx.keyspaces().size());
		assertEquals(789, (int) tx.get("MyAwesomeKeyspace", "third"));
	}

	@Test
	public void canExportAndImportIndexers() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("firstname", new FirstNameIndexer());
		db.getIndexManager().addIndexer("lastname", new LastNameIndexer());
		db.getIndexManager().addIndexer("nickname", new NicknameIndexer());
		db.getIndexManager().reindexAll();

		long afterFirstCommit = -1L;

		{ // first transaction
			ChronoDBTransaction tx = db.tx();
			tx.put("p1", new Person("John", "Doe", "Johnny", "JD"));
			tx.put("p2", new Person("Jane", "Doe", "Jenny", "JD"));
			tx.put("p3", new Person("Jack", "Doe", "Jacky", "JD"));
			tx.put("p4", new Person("John", "Smith", "Johnny", "JS"));
			tx.commit();
			afterFirstCommit = tx.getTimestamp();
		}

		{ // second transaction
			ChronoDBTransaction tx = db.tx();
			tx.remove("p3");
			tx.put("p2", new Person("Jane", "Smith", "Jenny", "JS"));
			tx.commit();
		}

		{ // make sure that the indexer state is consistent
			ChronoDBTransaction tx = db.tx();
			Set<String> JDs = tx.find().inDefaultKeyspace().where("nickname").containsIgnoreCase("JD").getKeysAsSet()
					// transform qualified keys into regular keys, everything resides in the same keyspace
					.stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
			assertEquals(Sets.newHashSet("p1"), JDs);
			Set<String> johns = tx.find().inDefaultKeyspace().where("firstname").isEqualToIgnoreCase("john")
					// transform qualified keys into regular keys, everything resides in the same keyspace
					.getKeysAsSet().stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
			assertEquals(Sets.newHashSet("p1", "p4"), johns);
		}

		// create the dump file
		File testDumpFile = this.createTestFile("Test.chronodump");

		// write the DB contents to the test file
		db.writeDump(testDumpFile);

		// print the file contents (for debugging)
		try {
			System.out.println(FileUtils.readFileToString(testDumpFile));
		} catch (IOException e) {
			e.printStackTrace();
		}

		// reinstantiate the DB to clear its contents
		ChronoDB db2 = this.reinstantiateDB();

		// read the dump
		db2.readDump(testDumpFile);

		{// make sure that the state is consistent after the first commit
			ChronoDBTransaction tx = db2.tx(afterFirstCommit);
			Set<String> JDs = tx.find().inDefaultKeyspace().where("nickname").containsIgnoreCase("jd").getKeysAsSet()
					.stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
			assertEquals(Sets.newHashSet("p1", "p2", "p3"), JDs);
			Set<String> johns = tx.find().inDefaultKeyspace().where("firstname").isEqualToIgnoreCase("john")
					.getKeysAsSet().stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
			assertEquals(Sets.newHashSet("p1", "p4"), johns);
		}
	}

	@Test
	public void canExportInPlainTextFormat() {
		ChronoDB db = this.getChronoDB();
		{ // insert test data
			ChronoDBTransaction tx = db.tx();
			tx.put("p1", new ExternalizablePerson("John", "Doe", "Johnny", "JD"));
			tx.put("p2", new ExternalizablePerson("Jane", "Doe", "Jenny", "JD"));
			tx.commit();
		}
		// create the file
		File dumpFile = this.createTestFile("Test.chronodump");
		// write to output
		db.writeDump(dumpFile);

		// read the dump into a string
		String dumpContents = null;
		try {
			dumpContents = FileUtils.readFileToString(dumpFile);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		// print the file contents (for debugging)
		System.out.println(dumpContents);

		// make sure that there are no binary entries in the resulting dump
		assertFalse(this.containsBinaryEntries(dumpContents));

		// read the dump
		ChronoDB db2 = this.reinstantiateDB();
		db2.readDump(dumpFile);

		{ // make sure that our data is present and accessible
			ChronoDBTransaction tx = db2.tx();
			ExternalizablePerson john = tx.get("p1");
			assertNotNull(john);
			assertEquals("John", john.getFirstName());
			assertEquals("Doe", john.getLastName());
			assertEquals(Sets.newHashSet("JD", "Johnny"), john.getNickNames());

			ExternalizablePerson jane = tx.get("p2");
			assertNotNull(jane);
			assertEquals("Jane", jane.getFirstName());
			assertEquals("Doe", jane.getLastName());
			assertEquals(Sets.newHashSet("JD", "Jenny"), jane.getNickNames());

		}
	}

	@Test
	public void canReadWriteZippedDump() {
		ChronoDB db = this.getChronoDB();
		db.getIndexManager().addIndexer("firstname", new FirstNameIndexer());
		db.getIndexManager().addIndexer("lastname", new LastNameIndexer());
		db.getIndexManager().addIndexer("nickname", new NicknameIndexer());
		db.getIndexManager().reindexAll();
		{ // add test data
			ChronoDBTransaction tx = db.tx();
			tx.put("p1", new Person("John", "Doe", "Johnny", "JD"));
			tx.put("p2", new Person("Jane", "Doe", "Jenny", "JD"));
			tx.commit();
		}
		// create the dump file
		File dumpFile = this.createTestFile("Test.chronodump.gzip");
		// write the dump data
		db.writeDump(dumpFile, DumpOption.ENABLE_GZIP);

		// make sure that the produced file is zipped
		assertTrue(ChronosFileUtils.isGZipped(dumpFile));

		// read the dump
		ChronoDB db2 = this.reinstantiateDB();
		db2.readDump(dumpFile);

		{ // make sure that our data is present and accessible
			ChronoDBTransaction tx = db2.tx();
			Person john = tx.get("p1");
			assertNotNull(john);
			assertEquals("John", john.getFirstName());
			assertEquals("Doe", john.getLastName());
			assertEquals(Sets.newHashSet("JD", "Johnny"), john.getNickNames());

			Person jane = tx.get("p2");
			assertNotNull(jane);
			assertEquals("Jane", jane.getFirstName());
			assertEquals("Doe", jane.getLastName());
			assertEquals(Sets.newHashSet("JD", "Jenny"), jane.getNickNames());
		}
	}

	@Test
	public void canExportAndImportWellKnownValues() {
		ChronoDB db = this.getChronoDB();
		{ // insert test data
			ChronoDBTransaction tx = db.tx();
			tx.put("set", Sets.newHashSet("Hello", "World"));
			tx.put("list", Lists.newArrayList(1, 2, 3, 4, 5));
			tx.put("enum", DayOfWeek.MONDAY);
			tx.put("enumList", Lists.newArrayList(DayOfWeek.MONDAY, DayOfWeek.FRIDAY));
			Map<String, Integer> map = Maps.newHashMap();
			map.put("pi", 31415);
			map.put("zero", 0);
			map.put("one", 1);
			tx.put("map", map);
			Map<String, Set<String>> multiMap = Maps.newHashMap();
			multiMap.put("persons", Sets.newHashSet("John", "Jane", "Jack"));
			multiMap.put("numbers", Sets.newHashSet("1", "2", "3"));
			tx.put("multimap", multiMap);
			tx.commit();
		}
		// create the file
		File dumpFile = this.createTestFile("Test.chronodump");
		// write to output
		db.writeDump(dumpFile);

		// read the dump into a string
		String dumpContents = null;
		try {
			dumpContents = FileUtils.readFileToString(dumpFile);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		// print the file contents (for debugging)
		System.out.println(dumpContents);

		// make sure that there are no binary entries in the resulting dump
		assertFalse(this.containsBinaryEntries(dumpContents));

		// read the dump
		ChronoDB db2 = this.reinstantiateDB();
		db2.readDump(dumpFile);

		{ // make sure that our data is present and accessible
			ChronoDBTransaction tx = db2.tx();
			assertEquals(Sets.newHashSet("Hello", "World"), tx.get("set"));
			assertEquals(Lists.newArrayList(1, 2, 3, 4, 5), tx.get("list"));
			assertEquals(DayOfWeek.MONDAY, tx.get("enum"));
			assertEquals(Lists.newArrayList(DayOfWeek.MONDAY, DayOfWeek.FRIDAY), tx.get("enumList"));
			Map<String, Integer> map = Maps.newHashMap();
			map.put("pi", 31415);
			map.put("zero", 0);
			map.put("one", 1);
			tx.put("map", map);
			assertEquals(map, tx.get("map"));
			Map<String, Set<String>> multiMap = Maps.newHashMap();
			multiMap.put("persons", Sets.newHashSet("John", "Jane", "Jack"));
			multiMap.put("numbers", Sets.newHashSet("1", "2", "3"));
			assertEquals(multiMap, tx.get("multimap"));
		}
	}

	@Test
	public void canFallBackToBinaryForUnknownValueTypes() {
		ChronoDB db = this.getChronoDB();
		{ // insert test data
			ChronoDBTransaction tx = db.tx();
			tx.put("p1", new Person("John", "Doe"));
			tx.put("p2", new Person("Jane", "Doe"));
			tx.put("persons", Lists.newArrayList(new Person("John", "Doe"), new Person("Jane", "Doe")));
			tx.commit();
		}
		// create the file
		File dumpFile = this.createTestFile("Test.chronodump");
		// write to output
		db.writeDump(dumpFile);

		// read the dump into a string
		String dumpContents = null;
		try {
			dumpContents = FileUtils.readFileToString(dumpFile);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		// print the file contents (for debugging)
		System.out.println(dumpContents);

		// make sure that there are binary entries (for the persons) in the dump
		assertTrue(this.containsBinaryEntries(dumpContents));

		// read the dump
		ChronoDB db2 = this.reinstantiateDB();
		db2.readDump(dumpFile);

		{ // make sure that our data is present and accessible
			ChronoDBTransaction tx = db2.tx();
			assertEquals(new Person("John", "Doe"), tx.get("p1"));
			assertEquals(new Person("Jane", "Doe"), tx.get("p2"));
			assertEquals(Lists.newArrayList(new Person("John", "Doe"), new Person("Jane", "Doe")), tx.get("persons"));
		}
	}

	@Test
	public void canRegisterDefaultConverters() {
		ChronoDB db = this.getChronoDB();
		{ // insert test data
			ChronoDBTransaction tx = db.tx();
			tx.put("p1", new Person("John", "Doe"));
			tx.put("p2", new Person("Jane", "Doe"));
			tx.commit();
		}
		// create the file
		File dumpFile = this.createTestFile("Test.chronodump");
		// write to output
		db.writeDump(dumpFile,
				// alias person class with a shorter name
				DumpOption.aliasHint(PersonDump.class, "person"),
				// use the default converter for persons
				DumpOption.defaultConverter(Person.class, new PersonDefaultConverter())
		// end of dump command
		);

		// read the dump into a string
		String dumpContents = null;
		try {
			dumpContents = FileUtils.readFileToString(dumpFile);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		// print the file contents (for debugging)
		System.out.println(dumpContents);

		// make sure that there are no binary entries (because our default converter should match)
		assertFalse(this.containsBinaryEntries(dumpContents));
		// make sure that our default converter does not appear in the output
		assertFalse(dumpContents.contains("PersonDefaultConverter"));

		// read the dump
		ChronoDB db2 = this.reinstantiateDB();
		db2.readDump(dumpFile,
				// alias person class with a shorter name
				DumpOption.aliasHint(PersonDump.class, "person"),
				// register the default converter (this time in the other direction
				DumpOption.defaultConverter(PersonDump.class, new PersonDefaultConverter())
		// end of dump read command
		);

		{ // make sure that our data is present and accessible
			ChronoDBTransaction tx = db2.tx();
			assertEquals(new Person("John", "Doe"), tx.get("p1"));
			assertEquals(new Person("Jane", "Doe"), tx.get("p2"));
		}
	}

	@Test
	public void canOverrideDefaultConverterWithAnnotation() {
		ChronoDB db = this.getChronoDB();
		{ // insert test data
			ChronoDBTransaction tx = db.tx();
			tx.put("p1", new ExternalizablePerson("John", "Doe"));
			tx.put("p2", new ExternalizablePerson("Jane", "Doe"));
			tx.commit();
		}
		// create the file
		File dumpFile = this.createTestFile("Test.chronodump");
		// write to output
		db.writeDump(dumpFile,
				// alias person class with a shorter name
				DumpOption.aliasHint(PersonDump.class, "person"),
				// use the default converter for persons
				DumpOption.defaultConverter(ExternalizablePerson.class, new PersonDefaultConverter())
		// end of dump command
		);

		// read the dump into a string
		String dumpContents = null;
		try {
			dumpContents = FileUtils.readFileToString(dumpFile);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		// print the file contents (for debugging)
		System.out.println(dumpContents);

		// make sure that there are no binary entries (because our default converter should match)
		assertFalse(this.containsBinaryEntries(dumpContents));
		// make sure that the annotation has overridden the default converter
		assertTrue(dumpContents.contains("PersonExternalizer"));

		// read the dump (NOTE: we do not register the default converter anymore, as there are no values for it)
		ChronoDB db2 = this.reinstantiateDB();
		db2.readDump(dumpFile,
				// alias person class with a shorter name
				DumpOption.aliasHint(PersonDump.class, "person")
		// end of dump read command
		);

		{ // make sure that our data is present and accessible
			ChronoDBTransaction tx = db2.tx();
			assertEquals(new ExternalizablePerson("John", "Doe"), tx.get("p1"));
			assertEquals(new ExternalizablePerson("Jane", "Doe"), tx.get("p2"));
		}
	}

	@Test
	public void canAccessCommitTimestampsAfterReadingDump() {
		ChronoDB db = this.getChronoDB();
		{ // insert test data
			ChronoDBTransaction tx = db.tx();
			tx.put("p1", new ExternalizablePerson("John", "Doe"));
			tx.put("p2", new ExternalizablePerson("Jane", "Doe"));
			tx.commit();

			tx.put("p3", new ExternalizablePerson("Jack", "Smith"));
			tx.commit();

			db.getBranchManager().createBranch("test");
			tx = db.tx("test");
			tx.put("p4", new ExternalizablePerson("Sarah", "Doe"));
			tx.commit();

			db.getBranchManager().createBranch("test", "testsub");
			tx = db.tx("testsub");
			tx.put("p5", new ExternalizablePerson("James", "Smith"));
			tx.commit();
		}
		// create the file
		File dumpFile = this.createTestFile("Test.chronodump");
		// write to output
		db.writeDump(dumpFile,
				// alias person class with a shorter name
				DumpOption.aliasHint(PersonDump.class, "person"),
				// use the default converter for persons
				DumpOption.defaultConverter(ExternalizablePerson.class, new PersonDefaultConverter())
		// end of dump command
		);

		// read the dump
		ChronoDB db2 = this.reinstantiateDB();
		db2.readDump(dumpFile,
				// alias person class with a shorter name
				DumpOption.aliasHint(PersonDump.class, "person")
		// end of dump read command
		);

		{ // assert that the commit history is accessible
			// there should be two commits on "master"
			assertEquals(2, db2.tx().countCommitTimestamps());
			// there should be one commit on "test"
			assertEquals(1, db2.tx("test").countCommitTimestamps());
			// there should be one commit on "testsub"
			assertEquals(1, db2.tx("testsub").countCommitTimestamps());
		}
	}

	@Test
	public void canAccessCommitMetadataAfterReadingDump() throws Exception {
		long commit1;
		long commit2;
		long commit3;
		long commit4;
		ChronoDB db = this.getChronoDB();
		{ // insert test data
			ChronoDBTransaction tx = db.tx();
			tx.put("p1", new ExternalizablePerson("John", "Doe"));
			tx.put("p2", new ExternalizablePerson("Jane", "Doe"));
			tx.commit("first");
			commit1 = tx.getTimestamp();

			tx.put("p3", new ExternalizablePerson("Jack", "Smith"));
			tx.commit("second");
			commit2 = tx.getTimestamp();

			db.getBranchManager().createBranch("test");
			tx = db.tx("test");
			tx.put("p4", new ExternalizablePerson("Sarah", "Doe"));
			tx.commit("third");
			commit3 = tx.getTimestamp();

			db.getBranchManager().createBranch("test", "testsub");
			tx = db.tx("testsub");
			tx.put("p5", new ExternalizablePerson("James", "Smith"));
			tx.commit("fourth");
			commit4 = tx.getTimestamp();
		}
		// create the file
		File dumpFile = this.createTestFile("Test.chronodump");
		// write to output
		db.writeDump(dumpFile,
				// alias person class with a shorter name
				DumpOption.aliasHint(PersonDump.class, "person"),
				// use the default converter for persons
				DumpOption.defaultConverter(ExternalizablePerson.class, new PersonDefaultConverter())
		// end of dump command
		);

		// read the dump
		ChronoDB db2 = this.reinstantiateDB();
		db2.readDump(dumpFile,
				// alias person class with a shorter name
				DumpOption.aliasHint(PersonDump.class, "person")
		// end of dump read command
		);

		{ // assert that the commit history is accessible
			// there should be two commits on "master"
			assertEquals(2, db2.tx().countCommitTimestamps());
			// there should be one commit on "test"
			assertEquals(1, db2.tx("test").countCommitTimestamps());
			// there should be one commit on "testsub"
			assertEquals(1, db2.tx("testsub").countCommitTimestamps());

			// assert that the metadata is accessible
			List<Entry<Long, Object>> commits = db2.tx().getCommitMetadataBefore(System.currentTimeMillis() + 1, 2);
			assertEquals(commit2, (long) commits.get(0).getKey());
			assertEquals("second", commits.get(0).getValue());
			assertEquals(commit1, (long) commits.get(1).getKey());
			assertEquals("first", commits.get(1).getValue());

			commits = db2.tx("test").getCommitMetadataBefore(System.currentTimeMillis() + 1, 1);
			assertEquals(commit3, (long) commits.get(0).getKey());
			assertEquals("third", commits.get(0).getValue());

			commits = db2.tx("testsub").getCommitMetadataBefore(System.currentTimeMillis() + 1, 1);
			assertEquals(commit4, (long) commits.get(0).getKey());
			assertEquals("fourth", commits.get(0).getValue());
		}
	}

	// =====================================================================================================================
	// HELPER METHODS
	// =====================================================================================================================

	private File createTestFile(final String filename) {
		File testDirectory = this.getTestDirectory();
		File testFile = new File(testDirectory, filename);
		try {
			testFile.createNewFile();
		} catch (IOException ioe) {
			fail(ioe.toString());
		}
		testFile.deleteOnExit();
		return testFile;
	}

	private boolean containsBinaryEntries(final String dumpContents) {
		return dumpContents.contains("<" + ChronoDBDumpFormat.ALIAS_NAME__CHRONODB_BINARY_ENTRY + ">");
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	private static class Person {

		private String firstName;
		private String lastName;
		private Set<String> nickNames = Sets.newHashSet();

		public Person() {
			// serialization constructor
		}

		public Person(final String firstName, final String lastName, final String... nicknames) {
			checkNotNull(firstName, "Precondition violation - argument 'firstName' must not be NULL!");
			checkNotNull(lastName, "Precondition violation - argument 'lastName' must not be NULL!");
			this.firstName = firstName;
			this.lastName = lastName;
			if (nicknames != null && nicknames.length > 0) {
				for (String nickname : nicknames) {
					this.nickNames.add(nickname);
				}
			}
		}

		public String getFirstName() {
			return this.firstName;
		}

		public String getLastName() {
			return this.lastName;
		}

		public Set<String> getNickNames() {
			return this.nickNames;
		}

		public void setFirstName(final String firstName) {
			this.firstName = firstName;
		}

		public void setLastName(final String lastName) {
			this.lastName = lastName;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (this.firstName == null ? 0 : this.firstName.hashCode());
			result = prime * result + (this.lastName == null ? 0 : this.lastName.hashCode());
			result = prime * result + (this.nickNames == null ? 0 : this.nickNames.hashCode());
			return result;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (this.getClass() != obj.getClass()) {
				return false;
			}
			Person other = (Person) obj;
			if (this.firstName == null) {
				if (other.firstName != null) {
					return false;
				}
			} else if (!this.firstName.equals(other.firstName)) {
				return false;
			}
			if (this.lastName == null) {
				if (other.lastName != null) {
					return false;
				}
			} else if (!this.lastName.equals(other.lastName)) {
				return false;
			}
			if (this.nickNames == null) {
				if (other.nickNames != null) {
					return false;
				}
			} else if (!this.nickNames.equals(other.nickNames)) {
				return false;
			}
			return true;
		}

	}

	@ChronosExternalizable(converterClass = PersonExternalizer.class)
	private static class ExternalizablePerson extends Person {

		public ExternalizablePerson() {
			super();
		}

		public ExternalizablePerson(final String firstName, final String lastName, final String... nicknames) {
			super(firstName, lastName, nicknames);
		}

	}

	// needs to be public; otherwise we can't instantiate it!
	public static class PersonExternalizer implements ChronoConverter<ExternalizablePerson, PersonDump> {

		public PersonExternalizer() {
		}

		@Override
		public PersonDump writeToOutput(final ExternalizablePerson person) {
			PersonDump dump = new PersonDump();
			dump.setFirstName(person.getFirstName());
			dump.setLastName(person.getLastName());
			dump.getNickNames().addAll(person.getNickNames());
			return dump;
		}

		@Override
		public ExternalizablePerson readFromInput(final PersonDump dump) {
			ExternalizablePerson person = new ExternalizablePerson();
			person.setFirstName(dump.getFirstName());
			person.setLastName(dump.getLastName());
			person.getNickNames().addAll(dump.getNickNames());
			return person;
		}

	}

	// needs to be public; otherwise we can't instantiate it!
	private static class PersonDefaultConverter implements ChronoConverter<Person, PersonDump> {

		public PersonDefaultConverter() {
		}

		@Override
		public PersonDump writeToOutput(final Person person) {
			PersonDump dump = new PersonDump();
			dump.setFirstName(person.getFirstName());
			dump.setLastName(person.getLastName());
			dump.getNickNames().addAll(person.getNickNames());
			return dump;
		}

		@Override
		public Person readFromInput(final PersonDump dump) {
			Person person = new Person();
			person.setFirstName(dump.getFirstName());
			person.setLastName(dump.getLastName());
			person.getNickNames().addAll(dump.getNickNames());
			return person;
		}

	}

	@SuppressWarnings("unused")
	private static class PersonDump {

		private String firstName;
		private String lastName;
		private Set<String> nickNames = Sets.newHashSet();

		public PersonDump() {
			// serialization constructor
		}

		public PersonDump(final String firstName, final String lastName, final String... nicknames) {
			checkNotNull(firstName, "Precondition violation - argument 'firstName' must not be NULL!");
			checkNotNull(lastName, "Precondition violation - argument 'lastName' must not be NULL!");
			this.firstName = firstName;
			this.lastName = lastName;
			if (nicknames != null && nicknames.length > 0) {
				for (String nickname : nicknames) {
					this.nickNames.add(nickname);
				}
			}
		}

		public String getFirstName() {
			return this.firstName;
		}

		public String getLastName() {
			return this.lastName;
		}

		public Set<String> getNickNames() {
			return this.nickNames;
		}

		public void setFirstName(final String firstName) {
			this.firstName = firstName;
		}

		public void setLastName(final String lastName) {
			this.lastName = lastName;
		}

	}

	private static abstract class PersonIndexer implements StringIndexer {

		@Override
		public boolean canIndex(final Object object) {
			return object != null && object instanceof Person;
		}

		@Override
		public Set<String> getIndexValues(final Object object) {
			Person person = (Person) object;
			return this.indexPerson(person);
		}

		protected abstract Set<String> indexPerson(Person person);

	}

	private static class FirstNameIndexer extends PersonIndexer {

		@Override
		public Set<String> indexPerson(final Person person) {
			return Collections.singleton(person.getFirstName());
		}

	}

	private static class LastNameIndexer extends PersonIndexer {

		@Override
		public Set<String> indexPerson(final Person person) {
			return Collections.singleton(person.getLastName());
		}

	}

	private static class NicknameIndexer extends PersonIndexer {

		@Override
		protected Set<String> indexPerson(final Person person) {
			return Sets.newHashSet(person.getNickNames());
		}

	}

}
