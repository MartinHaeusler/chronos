package org.chronos.chronodb.test.engine.maintenance.rollover;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronodb.test.util.model.person.Person;
import org.chronos.chronodb.test.util.model.person.PersonIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class RolloverTest extends AllChronoDBBackendsTest {

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "20")
    public void basicRolloverWorks() {
        ChronoDB db = this.getChronoDB();
        assumeRolloverIsSupported(db);

        // insert a couple of entries
        {
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("programming", "Foo", "Bar");
            tx.put("programming", "Number", "42");
            tx.put("person", "John", "Doe");
            tx.put("person", "Jane", "Doe");
            tx.commit();
        }
        long afterFirstCommit = System.currentTimeMillis();
        this.sleep(5);
        {
            ChronoDBTransaction tx = db.tx();
            tx.remove("person", "John");
            tx.remove("person", "Jane");
            tx.remove("programming", "Number");
            tx.put("programming", "Foo", "Baz");
            tx.put("person", "John", "Smith");
            tx.commit();
        }
        long afterSecondCommit = System.currentTimeMillis();
        this.sleep(5);

        // perform the rollover
        db.getMaintenanceManager().performRolloverOnMaster();

        // make sure that we can access the initial data
        {
            ChronoDBTransaction tx = db.tx(afterFirstCommit);
            assertEquals("World", tx.get("Hello"));
            assertEquals("Bar", tx.get("programming", "Foo"));
            assertEquals("42", tx.get("programming", "Number"));
            assertEquals("Doe", tx.get("person", "John"));
            assertEquals("Doe", tx.get("person", "Jane"));
            assertEquals(Sets.newHashSet("default", "programming", "person"), tx.keyspaces());
            assertEquals(Sets.newHashSet("Hello"), tx.keySet());
            assertEquals(Sets.newHashSet("Foo", "Number"), tx.keySet("programming"));
            assertEquals(Sets.newHashSet("John", "Jane"), tx.keySet("person"));
        }
        // make sure that the data after the second commit is still there
        {
            ChronoDBTransaction tx = db.tx(afterSecondCommit);
            assertEquals("World", tx.get("Hello"));
            assertEquals("Baz", tx.get("programming", "Foo"));
            assertEquals("Smith", tx.get("person", "John"));
            assertEquals(Sets.newHashSet("default", "programming", "person"), tx.keyspaces());
            assertEquals(Sets.newHashSet("Hello"), tx.keySet());
            assertEquals(Sets.newHashSet("Foo"), tx.keySet("programming"));
            assertEquals(Sets.newHashSet("John"), tx.keySet("person"));
        }

        // make sure that the head revision data is still there
        {
            ChronoDBTransaction tx = db.tx();
            assertEquals("World", tx.get("Hello"));
            assertEquals("Baz", tx.get("programming", "Foo"));
            assertEquals("Smith", tx.get("person", "John"));
            assertEquals(Sets.newHashSet("default", "programming", "person"), tx.keyspaces());
            assertEquals(Sets.newHashSet("Hello"), tx.keySet());
            assertEquals(Sets.newHashSet("Foo"), tx.keySet("programming"));
            assertEquals(Sets.newHashSet("John"), tx.keySet("person"));
        }

        // make sure that the rollover counted as "change" in the history
        {
            ChronoDBTransaction tx = db.tx();
            assertEquals(2, Iterators.size(tx.history("Hello")));
            assertEquals(3, Iterators.size(tx.history("programming", "Foo")));
        }

    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "20")
    public void multipleRolloverWork() {
        ChronoDB db = this.getChronoDB();
        assumeRolloverIsSupported(db);

        // insert a couple of entries
        {
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("programming", "Foo", "Bar");
            tx.put("programming", "Number", "42");
            tx.put("person", "John", "Doe");
            tx.put("person", "Jane", "Doe");
            tx.commit();
        }
        long afterFirstCommit = System.currentTimeMillis();
        this.sleep(5);
        {
            ChronoDBTransaction tx = db.tx();
            tx.remove("person", "John");
            tx.remove("person", "Jane");
            tx.remove("programming", "Number");
            tx.put("programming", "Foo", "Baz");
            tx.put("person", "John", "Smith");
            tx.commit();
        }
        long afterSecondCommit = System.currentTimeMillis();
        this.sleep(5);

        // perform the rollover
        db.getMaintenanceManager().performRolloverOnMaster();

        // do a commit on the new chunk
        {
            ChronoDBTransaction tx = db.tx();
            tx.put("math", "pi", "3.1415");
            tx.put("math", "e", "2.7182");
            tx.put("person", "John", "Johnson");
            tx.commit();
        }
        long afterThirdCommit = System.currentTimeMillis();
        this.sleep(5);

        // do another rollover
        db.getMaintenanceManager().performRolloverOnMaster();

        // make sure that we can access the initial data
        {
            ChronoDBTransaction tx = db.tx(afterFirstCommit);
            assertEquals("World", tx.get("Hello"));
            assertEquals("Bar", tx.get("programming", "Foo"));
            assertEquals("42", tx.get("programming", "Number"));
            assertEquals("Doe", tx.get("person", "John"));
            assertEquals("Doe", tx.get("person", "Jane"));
            assertEquals(Sets.newHashSet("default", "programming", "person"), tx.keyspaces());
            assertEquals(Sets.newHashSet("Hello"), tx.keySet());
            assertEquals(Sets.newHashSet("Foo", "Number"), tx.keySet("programming"));
            assertEquals(Sets.newHashSet("John", "Jane"), tx.keySet("person"));
        }
        // make sure that the data after the second commit is still there
        {
            ChronoDBTransaction tx = db.tx(afterSecondCommit);
            assertEquals("World", tx.get("Hello"));
            assertEquals("Baz", tx.get("programming", "Foo"));
            assertEquals("Smith", tx.get("person", "John"));
            assertEquals(Sets.newHashSet("default", "programming", "person"), tx.keyspaces());
            assertEquals(Sets.newHashSet("Hello"), tx.keySet());
            assertEquals(Sets.newHashSet("Foo"), tx.keySet("programming"));
            assertEquals(Sets.newHashSet("John"), tx.keySet("person"));
        }

        // make sure that the data after the third is still there
        {
            ChronoDBTransaction tx = db.tx(afterThirdCommit);
            assertEquals("World", tx.get("Hello"));
            assertEquals("Baz", tx.get("programming", "Foo"));
            assertEquals("Johnson", tx.get("person", "John"));
            assertEquals("3.1415", tx.get("math", "pi"));
            assertEquals("2.7182", tx.get("math", "e"));
            assertEquals(Sets.newHashSet("default", "programming", "person", "math"), tx.keyspaces());
            assertEquals(Sets.newHashSet("Hello"), tx.keySet());
            assertEquals(Sets.newHashSet("Foo"), tx.keySet("programming"));
            assertEquals(Sets.newHashSet("John"), tx.keySet("person"));
            assertEquals(Sets.newHashSet("pi", "e"), tx.keySet("math"));
        }

        // make sure that the head revision data is accessible
        {
            ChronoDBTransaction tx = db.tx();
            assertEquals("World", tx.get("Hello"));
            assertEquals("Baz", tx.get("programming", "Foo"));
            assertEquals("Johnson", tx.get("person", "John"));
            assertEquals("3.1415", tx.get("math", "pi"));
            assertEquals("2.7182", tx.get("math", "e"));
            assertEquals(Sets.newHashSet("default", "programming", "person", "math"), tx.keyspaces());
            assertEquals(Sets.newHashSet("Hello"), tx.keySet());
            assertEquals(Sets.newHashSet("Foo"), tx.keySet("programming"));
            assertEquals(Sets.newHashSet("John"), tx.keySet("person"));
            assertEquals(Sets.newHashSet("pi", "e"), tx.keySet("math"));
        }

        // make sure that the rollover counted as "change" in the history
        {
            ChronoDBTransaction tx = db.tx();
            assertEquals(3, Iterators.size(tx.history("Hello")));
            assertEquals(4, Iterators.size(tx.history("programming", "Foo")));
            assertEquals(5, Iterators.size(tx.history("person", "John")));
        }
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "20")
    @InstantiateChronosWith(property = ChronoDBConfiguration.ASSUME_CACHE_VALUES_ARE_IMMUTABLE, value = "false")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "10")
    @InstantiateChronosWith(property = ChronoDBConfiguration.DUPLICATE_VERSION_ELIMINATION_MODE, value = "off")
    public void secondaryIndexingWorksWithBasicRollover() {
        ChronoDB db = this.getChronoDB();
        assumeRolloverIsSupported(db);
        db.getIndexManager().addIndexer("firstName", PersonIndexer.firstName());
        db.getIndexManager().addIndexer("lastName", PersonIndexer.lastName());
        db.getIndexManager().addIndexer("favoriteColor", PersonIndexer.favoriteColor());
        db.getIndexManager().addIndexer("hobbies", PersonIndexer.hobbies());
        db.getIndexManager().addIndexer("pets", PersonIndexer.pets());
        db.getIndexManager().reindexAll();

        // add a couple of persons
        {
            ChronoDBTransaction tx = db.tx();
            Person johnDoe = new Person("John", "Doe");
            johnDoe.setFavoriteColor("red");
            johnDoe.setHobbies("swimming", "skiing", "reading");
            johnDoe.setPets("cat");
            Person janeDoe = new Person("Jane", "Doe");
            janeDoe.setFavoriteColor("blue");
            janeDoe.setHobbies("skiing", "cinema", "biking");
            janeDoe.setPets("cat");
            Person sarahDoe = new Person("Sarah", "Doe");
            sarahDoe.setFavoriteColor("green");
            sarahDoe.setHobbies("swimming", "reading", "cinema");
            sarahDoe.setPets("cat");
            tx.put("theDoes", "p1", johnDoe);
            tx.put("theDoes", "p2", janeDoe);
            tx.put("theDoes", "p3", sarahDoe);
            Person jackSmith = new Person("Jack", "Smith");
            jackSmith.setFavoriteColor("orange");
            jackSmith.setHobbies("games");
            jackSmith.setPets("cat", "dog", "fish");
            Person johnSmith = new Person("John", "Smith");
            johnSmith.setFavoriteColor("yellow");
            johnSmith.setHobbies("reading");
            johnSmith.setPets("cat", "dog");
            tx.put("theSmiths", "p1", jackSmith);
            tx.put("theSmiths", "p2", johnSmith);
            tx.commit();
        }
        long afterFirstCommit = System.currentTimeMillis();
        this.sleep(5);

        // make a couple of modifications
        {
            ChronoDBTransaction tx = db.tx();
            // sarah gets a dog
            Person sarahDoe = tx.get("theDoes", "p3");
            sarahDoe.getPets().add("dog");
            tx.put("theDoes", "p3", sarahDoe);
            // john smith goes on vacation...
            tx.remove("theSmiths", "p2");
            tx.commit();
        }
        long afterSecondCommit = System.currentTimeMillis();

        // do the rollover
        db.getMaintenanceManager().performRolloverOnMaster();

        // run a couple of queries on the initial state
        {
            ChronoDBTransaction tx = db.tx(afterFirstCommit);
            Set<String> personsWithJInFirstName = tx.find().inKeyspace("theDoes")
                    // find all persons with a "j" in the first name
                    .where("firstName").containsIgnoreCase("j")
                    // transform to plain key set
                    .getKeysAsSet().stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
            assertEquals(Sets.newHashSet("p1", "p2"), personsWithJInFirstName);
            Set<String> personsWhoLikeReading = tx.find().inKeyspace("theDoes")
                    // find all persons that have reading as a hobby
                    .where("hobbies").contains("reading")
                    // transform to plain key set
                    .getKeysAsSet().stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
            assertEquals(Sets.newHashSet("p1", "p3"), personsWhoLikeReading);
        }
        // run a couple of queries after the first commit
        {
            ChronoDBTransaction tx = db.tx(afterSecondCommit);
            Set<String> personsWithJInFirstName = tx.find().inKeyspace("theDoes")
                    // find all persons with a "j" in the first name
                    .where("firstName").containsIgnoreCase("j")
                    // transform to plain key set
                    .getKeysAsSet().stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
            assertEquals(Sets.newHashSet("p1", "p2"), personsWithJInFirstName);
            Set<String> personsWhoLikeReading = tx.find().inKeyspace("theDoes")
                    // find all persons that have reading as a hobby
                    .where("hobbies").contains("reading")
                    // transform to plain key set
                    .getKeysAsSet().stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
            assertEquals(Sets.newHashSet("p1", "p3"), personsWhoLikeReading);
            Set<String> doesWhoHaveADog = tx.find().inKeyspace("theDoes")
                    // find all persons who have a dog
                    .where("pets").contains("dog")
                    // transform to plain key set
                    .getKeysAsSet().stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
            assertEquals(Sets.newHashSet("p3"), doesWhoHaveADog);
            Set<String> smithsWhoHaveADog = tx.find().inKeyspace("theSmiths")
                    // find all persons who have a dog
                    .where("pets").contains("dog")
                    // transform to plain key set
                    .getKeysAsSet().stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
            assertEquals(Sets.newHashSet("p1"), smithsWhoHaveADog);
        }

        // run a couple of queries on the head revision
        {
            ChronoDBTransaction tx = db.tx();
            Set<String> personsWithJInFirstName = tx.find().inKeyspace("theDoes")
                    // find all persons with a "j" in the first name
                    .where("firstName").containsIgnoreCase("j")
                    // transform to plain key set
                    .getKeysAsSet().stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
            assertEquals(Sets.newHashSet("p1", "p2"), personsWithJInFirstName);
            Set<String> personsWhoLikeReading = tx.find().inKeyspace("theDoes")
                    // find all persons that have reading as a hobby
                    .where("hobbies").contains("reading")
                    // transform to plain key set
                    .getKeysAsSet().stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
            assertEquals(Sets.newHashSet("p1", "p3"), personsWhoLikeReading);
            Set<String> doesWhoHaveADog = tx.find().inKeyspace("theDoes")
                    // find all persons who have a dog
                    .where("pets").contains("dog")
                    // transform to plain key set
                    .getKeysAsSet().stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
            assertEquals(Sets.newHashSet("p3"), doesWhoHaveADog);
            Set<String> smithsWhoHaveADog = tx.find().inKeyspace("theSmiths")
                    // find all persons who have a dog
                    .where("pets").contains("dog")
                    // transform to plain key set
                    .getKeysAsSet().stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
            assertEquals(Sets.newHashSet("p1"), smithsWhoHaveADog);
        }
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100000")
    public void performingARolloverPreservesEntrySetWithCache() {
        ChronoDB db = this.getChronoDB();
        assumeRolloverIsSupported(db);
        this.runPerformingARolloverPerservesEntrySetTest(db);
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "false")
    public void performingARolloverPreservesEntrySetWithoutCache() {
        ChronoDB db = this.getChronoDB();
        assumeRolloverIsSupported(db);
        this.runPerformingARolloverPerservesEntrySetTest(db);
    }

    private void runPerformingARolloverPerservesEntrySetTest(final ChronoDB db) {

        // in this test, we work with two keyspaces, each containing four keys:
        // 1: this key will never be modified and serves as a sanity check.
        // 2: this key will be overwritten with a new value before the rollover.
        // 3: this key will be removed before the rollover
        // 4: this key will first be overwritten and then removed before the rollover.
        // both keyspaces contain the same keys, except prefixed by 'a' or 'b', respectively.

        {
            ChronoDBTransaction tx = db.tx();
            // keyspace A
            tx.put("keyspaceA", "a1", "a");
            tx.put("keyspaceA", "a2", "b");
            tx.put("keyspaceA", "a3", "c");
            tx.put("keyspaceA", "a4", "d");
            // keyspace B
            tx.put("keyspaceB", "b1", "a");
            tx.put("keyspaceB", "b2", "b");
            tx.put("keyspaceB", "b3", "c");
            tx.put("keyspaceB", "b4", "d");
            tx.commit();
        }

        // check that the commit worked as intended
        assertEquals(Sets.newHashSet("a1", "a2", "a3", "a4"), db.tx().keySet("keyspaceA"));
        assertEquals(Sets.newHashSet("b1", "b2", "b3", "b4"), db.tx().keySet("keyspaceB"));

        // perform the first set of modifications
        {
            ChronoDBTransaction tx = db.tx();
            // keyspace A
            tx.put("keyspaceA", "a2", "modified");
            tx.remove("keyspaceA", "a3");
            tx.put("keyspaceA", "a4", "modified");
            // keyspace B
            tx.put("keyspaceB", "b2", "modified");
            tx.remove("keyspaceB", "b3");
            tx.put("keyspaceB", "b4", "modified");
            tx.commit();
        }

        // check that the modifications produced the expected result
        assertEquals(Sets.newHashSet("a1", "a2", "a4"), db.tx().keySet("keyspaceA"));
        assertEquals(Sets.newHashSet("b1", "b2", "b4"), db.tx().keySet("keyspaceB"));

        // perform the second set of modifications
        {
            ChronoDBTransaction tx = db.tx();
            // keyspace A
            tx.remove("keyspaceA", "a4");
            // keyspace B
            tx.remove("keyspaceB", "b4");
            tx.commit();
        }

        // check that the modifications produced the expected result
        assertEquals(Sets.newHashSet("a1", "a2"), db.tx().keySet("keyspaceA"));
        assertEquals(Sets.newHashSet("b1", "b2"), db.tx().keySet("keyspaceB"));

        long beforeRollover = db.getBranchManager().getMasterBranch().getNow();

        // perform the rollover
        db.getMaintenanceManager().performRolloverOnMaster();

        // make sure that we can request the same key set before and after the rollover
        assertEquals(Sets.newHashSet("a1", "a2"), db.tx(beforeRollover).keySet("keyspaceA"));
        assertEquals(Sets.newHashSet("b1", "b2"), db.tx(beforeRollover).keySet("keyspaceB"));
        assertEquals(Sets.newHashSet("a1", "a2"), db.tx().keySet("keyspaceA"));
        assertEquals(Sets.newHashSet("b1", "b2"), db.tx().keySet("keyspaceB"));

        // assert that the "get" operations deliver the same results

        // before rollover
        assertEquals("a", db.tx(beforeRollover).get("keyspaceA", "a1"));
        assertEquals("modified", db.tx(beforeRollover).get("keyspaceA", "a2"));
        assertFalse(db.tx(beforeRollover).exists("keyspaceA", "a3"));
        assertFalse(db.tx(beforeRollover).exists("keyspaceA", "a4"));
        assertEquals("a", db.tx(beforeRollover).get("keyspaceB", "b1"));
        assertEquals("modified", db.tx(beforeRollover).get("keyspaceB", "b2"));
        assertFalse(db.tx(beforeRollover).exists("keyspaceB", "b3"));
        assertFalse(db.tx(beforeRollover).exists("keyspaceB", "b4"));

        // after rollover
        assertEquals("a", db.tx().get("keyspaceA", "a1"));
        assertEquals("modified", db.tx().get("keyspaceA", "a2"));
        assertFalse(db.tx().exists("keyspaceA", "a3"));
        assertFalse(db.tx().exists("keyspaceA", "a4"));
        assertEquals("a", db.tx().get("keyspaceB", "b1"));
        assertEquals("modified", db.tx().get("keyspaceB", "b2"));
        assertFalse(db.tx().exists("keyspaceB", "b3"));
        assertFalse(db.tx().exists("keyspaceB", "b4"));

        // perform the third set of changes (after rollover)
        {
            ChronoDBTransaction tx = db.tx();
            // keyspace A
            tx.put("keyspaceA", "a2", "modified2");
            tx.put("keyspaceA", "a5", "inserted");
            // keyspace B
            tx.put("keyspaceB", "b2", "modified2");
            tx.put("keyspaceB", "b5", "inserted");
            tx.commit();
        }

        assertEquals(Sets.newHashSet("a1", "a2", "a5"), db.tx().keySet("keyspaceA"));
        assertEquals(Sets.newHashSet("b1", "b2", "b5"), db.tx().keySet("keyspaceB"));
        assertEquals("modified2", db.tx().get("keyspaceA", "a2"));
        assertEquals("modified2", db.tx().get("keyspaceB", "b2"));

    }

    @Test
    public void performingARolloverLimitsValidTo() {
        ChronoDB db = this.getChronoDB();
        assumeRolloverIsSupported(db);

        { // insert test data
            ChronoDBTransaction tx = db.tx();
            tx.put("pi", "3.1415");
            tx.put("hello", "world");
            tx.commit();
        }

        long beforeRollover = db.getBranchManager().getMasterBranch().getNow();

        db.getMaintenanceManager().performRolloverOnMaster();

        long now = db.getBranchManager().getMasterBranch().getNow();

        {// check validity ranges
            // open a transaction on the old state before the rollover
            ChronoDBTransaction tx = db.tx(beforeRollover);
            BranchInternal masterBranch = (BranchInternal) db.getBranchManager().getMasterBranch();
            TemporalKeyValueStore tkvs = masterBranch.getTemporalKeyValueStore();
            GetResult<Object> getResult = tkvs.performRangedGet(tx, QualifiedKey.createInDefaultKeyspace("hello"));
            assertTrue(getResult.getPeriod().getUpperBound() <= now);
        }

    }

    // =================================================================================================================
    // INTERNAL HELPER METHODS
    // =================================================================================================================

    private static void assumeRolloverIsSupported(final ChronoDB db) {
        Assume.assumeTrue(db.getMaintenanceManager().isRolloverSupported());
    }
}
