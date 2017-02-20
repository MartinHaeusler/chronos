package org.chronos.chronodb.test.engine.indexing.diff;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Set;

import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.internal.impl.index.diff.IndexValueDiff;
import org.chronos.chronodb.internal.impl.index.diff.IndexingUtils;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.chronodb.test.util.model.person.Person;
import org.chronos.chronodb.test.util.model.person.PersonIndexer;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

@Category(UnitTest.class)
public class IndexingUtilsTest extends ChronoDBUnitTest {

	// =====================================================================================================================
	// GET INDEX VALUES TESTS
	// =====================================================================================================================

	@Test
	public void canGetIndexValuesForObject() {
		Person johnDoe = createJohnDoe();
		Set<String> firstNameValues = IndexingUtils
				.getIndexedValuesForObject(Collections.singleton(PersonIndexer.firstName()), johnDoe);
		assertEquals(Sets.newHashSet("John"), firstNameValues);
		Set<String> lastNameValues = IndexingUtils
				.getIndexedValuesForObject(Collections.singleton(PersonIndexer.lastName()), johnDoe);
		assertEquals(Sets.newHashSet("Doe"), lastNameValues);
	}

	@Test
	public void canGetIndexValuesForNullValuedFields() {
		Person johnDoe = createJohnDoe();
		Set<String> favoriteColorValues = IndexingUtils
				.getIndexedValuesForObject(Collections.singleton(PersonIndexer.favoriteColor()), johnDoe);
		assertEquals(Collections.emptySet(), favoriteColorValues);
	}

	@Test
	public void canGetIndexValuesForEmptyCollectionFields() {
		Person johnDoe = createJohnDoe();
		Set<String> petsValues = IndexingUtils.getIndexedValuesForObject(Collections.singleton(PersonIndexer.pets()),
				johnDoe);
		assertEquals(Collections.emptySet(), petsValues);
	}

	@Test
	public void indexedValuesDoNotContainNullOrEmptyString() {
		Person johnDoe = createJohnDoe();
		Set<String> hobbiesValues = IndexingUtils
				.getIndexedValuesForObject(Collections.singleton(PersonIndexer.hobbies()), johnDoe);
		assertEquals(Sets.newHashSet("Swimming", "Skiing"), hobbiesValues);
	}

	@Test
	public void attemptingToGetIndexValuesWithoutIndexerProducesEmptySet() {
		Person johnDoe = createJohnDoe();
		Set<String> hobbiesValues2 = IndexingUtils.getIndexedValuesForObject(Collections.emptySet(), johnDoe);
		assertEquals(Collections.emptySet(), hobbiesValues2);
	}

	// =====================================================================================================================
	// DIFF CALCULATION TESTS
	// =====================================================================================================================

	@Test
	public void canCalculateAdditiveDiff() {
		Person p1 = new Person();
		p1.setFirstName("John");
		p1.setLastName("Doe");
		p1.setHobbies("Swimming", "Skiing");
		Person p2 = new Person();
		p2.setFirstName("John");
		p2.setLastName("Doe");
		p2.setHobbies("Swimming", "Skiing", "Cinema", "Reading");
		p2.setPets("Cat", "Dog", "Fish");
		SetMultimap<String, ChronoIndexer> indexers = HashMultimap.create();
		indexers.put("firstName", PersonIndexer.firstName());
		indexers.put("lastName", PersonIndexer.lastName());
		indexers.put("hobbies", PersonIndexer.hobbies());
		indexers.put("pets", PersonIndexer.pets());
		IndexValueDiff diff = IndexingUtils.calculateDiff(indexers, p1, p2);
		assertNotNull(diff);
		assertFalse(diff.isEntryAddition());
		assertFalse(diff.isEntryRemoval());
		assertTrue(diff.isEntryUpdate());
		assertTrue(diff.isAdditive());
		assertFalse(diff.isSubtractive());
		assertFalse(diff.isMixed());
		assertFalse(diff.isEmpty());
		assertEquals(Sets.newHashSet("hobbies", "pets"), diff.getChangedIndices());
		assertEquals(Sets.newHashSet("Cinema", "Reading"), diff.getAdditions("hobbies"));
		assertEquals(Sets.newHashSet("Cat", "Dog", "Fish"), diff.getAdditions("pets"));
	}

	@Test
	public void canCalculateSubtractiveDiff() {
		Person p1 = new Person();
		p1.setFirstName("John");
		p1.setLastName("Doe");
		p1.setHobbies("Swimming", "Skiing", "Cinema", "Reading");
		p1.setPets("Cat", "Dog", "Fish");
		Person p2 = new Person();
		p2.setFirstName("John");
		p2.setLastName("Doe");
		p2.setHobbies("Swimming", "Skiing");
		SetMultimap<String, ChronoIndexer> indexers = HashMultimap.create();
		indexers.put("firstName", PersonIndexer.firstName());
		indexers.put("lastName", PersonIndexer.lastName());
		indexers.put("hobbies", PersonIndexer.hobbies());
		indexers.put("pets", PersonIndexer.pets());
		IndexValueDiff diff = IndexingUtils.calculateDiff(indexers, p1, p2);
		assertNotNull(diff);
		assertFalse(diff.isEntryAddition());
		assertFalse(diff.isEntryRemoval());
		assertTrue(diff.isEntryUpdate());
		assertFalse(diff.isAdditive());
		assertTrue(diff.isSubtractive());
		assertFalse(diff.isMixed());
		assertFalse(diff.isEmpty());
		assertEquals(Sets.newHashSet("hobbies", "pets"), diff.getChangedIndices());
		assertEquals(Sets.newHashSet("Cinema", "Reading"), diff.getRemovals("hobbies"));
		assertEquals(Sets.newHashSet("Cat", "Dog", "Fish"), diff.getRemovals("pets"));
	}

	@Test
	public void canCalculateEntryAdditionDiff() {
		Person johnDoe = createJohnDoe();
		SetMultimap<String, ChronoIndexer> indexers = HashMultimap.create();
		indexers.put("firstName", PersonIndexer.firstName());
		indexers.put("lastName", PersonIndexer.lastName());
		indexers.put("favoriteColor", PersonIndexer.favoriteColor());
		indexers.put("pets", PersonIndexer.pets());
		indexers.put("hobbies", PersonIndexer.hobbies());
		// simulate the addition of John Doe
		IndexValueDiff diff = IndexingUtils.calculateDiff(indexers, null, johnDoe);
		assertNotNull(diff);
		assertTrue(diff.isEntryAddition());
		assertFalse(diff.isEntryRemoval());
		assertFalse(diff.isEntryUpdate());
		assertTrue(diff.isAdditive());
		assertFalse(diff.isSubtractive());
		assertFalse(diff.isMixed());
		assertFalse(diff.isEmpty());
		assertEquals(Sets.newHashSet("firstName", "lastName", "hobbies"), diff.getChangedIndices());

		assertEquals(Sets.newHashSet("John"), diff.getAdditions("firstName"));
		assertEquals(Collections.emptySet(), diff.getRemovals("firstName"));

		assertEquals(Sets.newHashSet("Doe"), diff.getAdditions("lastName"));
		assertEquals(Collections.emptySet(), diff.getRemovals("lastName"));

		assertEquals(Sets.newHashSet("Swimming", "Skiing"), diff.getAdditions("hobbies"));
		assertEquals(Collections.emptySet(), diff.getRemovals("hobbies"));
	}

	@Test
	public void canCalculateEntryRemovalDiff() {
		Person johnDoe = createJohnDoe();
		SetMultimap<String, ChronoIndexer> indexers = HashMultimap.create();
		indexers.put("firstName", PersonIndexer.firstName());
		indexers.put("lastName", PersonIndexer.lastName());
		indexers.put("favoriteColor", PersonIndexer.favoriteColor());
		indexers.put("pets", PersonIndexer.pets());
		indexers.put("hobbies", PersonIndexer.hobbies());
		// simulate the deletion of John Doe
		IndexValueDiff diff = IndexingUtils.calculateDiff(indexers, johnDoe, null);
		assertNotNull(diff);
		assertFalse(diff.isEntryAddition());
		assertTrue(diff.isEntryRemoval());
		assertFalse(diff.isEntryUpdate());
		assertFalse(diff.isAdditive());
		assertTrue(diff.isSubtractive());
		assertFalse(diff.isMixed());
		assertFalse(diff.isEmpty());
		assertEquals(Sets.newHashSet("firstName", "lastName", "hobbies"), diff.getChangedIndices());

		assertEquals(Collections.emptySet(), diff.getAdditions("firstName"));
		assertEquals(Sets.newHashSet("John"), diff.getRemovals("firstName"));

		assertEquals(Collections.emptySet(), diff.getAdditions("lastName"));
		assertEquals(Sets.newHashSet("Doe"), diff.getRemovals("lastName"));

		assertEquals(Collections.emptySet(), diff.getAdditions("hobbies"));
		assertEquals(Sets.newHashSet("Swimming", "Skiing"), diff.getRemovals("hobbies"));
	}

	@Test
	public void canCalculateMixedDiff() {
		Person p1 = new Person();
		p1.setFirstName("John");
		p1.setLastName("Doe");
		p1.setHobbies("Swimming", "Skiing");
		Person p2 = new Person();
		p2.setFirstName("John");
		p2.setLastName("Smith");
		p2.setHobbies("Skiing", "Cinema");
		SetMultimap<String, ChronoIndexer> indexers = HashMultimap.create();
		indexers.put("firstName", PersonIndexer.firstName());
		indexers.put("lastName", PersonIndexer.lastName());
		indexers.put("hobbies", PersonIndexer.hobbies());
		IndexValueDiff diff = IndexingUtils.calculateDiff(indexers, p1, p2);
		assertNotNull(diff);
		assertFalse(diff.isEntryAddition());
		assertFalse(diff.isEntryRemoval());
		assertTrue(diff.isEntryUpdate());
		assertFalse(diff.isAdditive());
		assertFalse(diff.isSubtractive());
		assertTrue(diff.isMixed());
		assertFalse(diff.isEmpty());
		assertEquals(Collections.emptySet(), diff.getAdditions("firstName"));
		assertEquals(Collections.emptySet(), diff.getRemovals("firstName"));
		assertFalse(diff.isIndexChanged("firstName"));
		assertEquals(Collections.singleton("Smith"), diff.getAdditions("lastName"));
		assertEquals(Collections.singleton("Doe"), diff.getRemovals("lastName"));
		assertTrue(diff.isIndexChanged("lastName"));
		assertEquals(Collections.singleton("Cinema"), diff.getAdditions("hobbies"));
		assertEquals(Collections.singleton("Swimming"), diff.getRemovals("hobbies"));
		assertTrue(diff.isIndexChanged("hobbies"));
		assertEquals(Sets.newHashSet("lastName", "hobbies"), diff.getChangedIndices());
		assertFalse(diff.isEntryAddition());
		assertFalse(diff.isEntryRemoval());
		assertTrue(diff.isEntryUpdate());
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private static Person createJohnDoe() {
		Person johnDoe = new Person();
		johnDoe.setFirstName("John");
		johnDoe.setLastName("Doe");
		johnDoe.setFavoriteColor(null);
		johnDoe.setHobbies(Sets.newHashSet("Swimming", "Skiing", "   ", "", null));
		johnDoe.setPets(Collections.emptySet());
		return johnDoe;
	}
}
