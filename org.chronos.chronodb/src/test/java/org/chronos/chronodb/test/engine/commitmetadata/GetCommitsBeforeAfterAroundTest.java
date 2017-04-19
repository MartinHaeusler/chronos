package org.chronos.chronodb.test.engine.commitmetadata;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(IntegrationTest.class)
public class GetCommitsBeforeAfterAroundTest extends AllChronoDBBackendsTest {

	// =================================================================================================================
	// COMMITS AROUND
	// =================================================================================================================

	@Test
	public void commitsAroundWorksIfNoCommitsArePresent() {
		ChronoDB db = this.getChronoDB();
		List<Entry<Long, Object>> commits = db.tx().getCommitMetadataAround(0, 10);
		assertNotNull(commits);
		assertTrue(commits.isEmpty());
	}

	@Test
	public void commitsAroundWorksIfNotEnoughCommitsArePresentOnEitherSide() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		List<Entry<Long, Object>> commitsAround = tx.getCommitMetadataAround(afterSecondCommit, 5);
		assertEquals(3, commitsAround.size());
		assertEquals(afterThirdCommit, (long) commitsAround.get(0).getKey());
		assertEquals("Third", commitsAround.get(0).getValue());
		assertEquals(afterSecondCommit, (long) commitsAround.get(1).getKey());
		assertEquals("Second", commitsAround.get(1).getValue());
		assertEquals(afterFirstCommit, (long) commitsAround.get(2).getKey());
		assertEquals("First", commitsAround.get(2).getValue());
	}

	@Test
	public void commitsAroundWorksIfExactlyEnoughElementsArePresentOnBothSides() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		List<Entry<Long, Object>> commitsAround = tx.getCommitMetadataAround(afterSecondCommit, 3);
		assertEquals(3, commitsAround.size());
		assertEquals(afterThirdCommit, (long) commitsAround.get(0).getKey());
		assertEquals("Third", commitsAround.get(0).getValue());
		assertEquals(afterSecondCommit, (long) commitsAround.get(1).getKey());
		assertEquals("Second", commitsAround.get(1).getValue());
		assertEquals(afterFirstCommit, (long) commitsAround.get(2).getKey());
		assertEquals("First", commitsAround.get(2).getValue());
	}

	@Test
	public void commitsAroundWorksIfNotEnoughElementsArePresentBefore() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));
		expected.add(Pair.of(afterFirstCommit, "First"));

		List<Entry<Long, Object>> commitsAround = tx.getCommitMetadataAround(afterSecondCommit, 4);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitsAroundWorksIfNotEnoughElementsArePresentAfter() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		long afterFifthCommit = tx.getTimestamp();

		tx.put("6", "Six");
		tx.commit("Sixth");

		long afterSixthCommit = tx.getTimestamp();

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterSixthCommit, "Sixth"));
		expected.add(Pair.of(afterFifthCommit, "Fifth"));
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));

		List<Entry<Long, Object>> commitsAround = tx.getCommitMetadataAround(afterFifthCommit, 5);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitsAroundWorksIfMoreElementsThanNecessaryArePresentOnBothSides() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		tx.put("6", "Six");
		tx.commit("Sixth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));

		List<Entry<Long, Object>> commitsAround = tx.getCommitMetadataAround(afterThirdCommit, 3);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitsAroundWorksIfTimestampIsNotExactlyMatchingACommit() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		tx.put("Foo", "Bar");
		tx.commit("Second");

		this.sleep(10);

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		this.sleep(10);

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		this.sleep(10);

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		this.sleep(10);

		long afterFifthCommit = tx.getTimestamp();

		tx.put("6", "Six");
		tx.commit("Sixth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterFifthCommit, "Fifth"));
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));

		long requestTimestamp = (afterFourthCommit + afterThirdCommit) / 2;

		List<Entry<Long, Object>> commitsAround = tx.getCommitMetadataAround(requestTimestamp, 3);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitTimestampsAroundWorksIfNotEnoughCommitsArePresentOnEitherSide() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		List<Long> commitsAround = tx.getCommitTimestampsAround(afterSecondCommit, 5);
		assertEquals(3, commitsAround.size());
		assertEquals(afterThirdCommit, (long) commitsAround.get(0));
		assertEquals(afterSecondCommit, (long) commitsAround.get(1));
		assertEquals(afterFirstCommit, (long) commitsAround.get(2));
	}

	@Test
	public void commitTimestampsAroundWorksIfExactlyEnoughElementsArePresentOnBothSides() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		List<Long> commitsAround = tx.getCommitTimestampsAround(afterSecondCommit, 3);
		assertEquals(3, commitsAround.size());
		assertEquals(afterThirdCommit, (long) commitsAround.get(0));
		assertEquals(afterSecondCommit, (long) commitsAround.get(1));
		assertEquals(afterFirstCommit, (long) commitsAround.get(2));
	}

	@Test
	public void commitTimestampsAroundWorksIfNotEnoughElementsArePresentBefore() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);
		expected.add(afterFirstCommit);

		List<Long> commitsAround = tx.getCommitTimestampsAround(afterSecondCommit, 4);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitTimestampsAroundWorksIfNotEnoughElementsArePresentAfter() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		long afterFifthCommit = tx.getTimestamp();

		tx.put("6", "Six");
		tx.commit("Sixth");

		long afterSixthCommit = tx.getTimestamp();

		List<Long> expected = Lists.newArrayList();
		expected.add(afterSixthCommit);
		expected.add(afterFifthCommit);
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);

		List<Long> commitsAround = tx.getCommitTimestampsAround(afterFifthCommit, 5);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitTimestampsAroundWorksIfMoreElementsThanNecessaryArePresentOnBothSides() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		tx.put("6", "Six");
		tx.commit("Sixth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);

		List<Long> commitsAround = tx.getCommitTimestampsAround(afterThirdCommit, 3);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitTimestampsAroundWorksIfTimestampIsNotExactlyMatchingACommit() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		tx.put("Foo", "Bar");
		tx.commit("Second");

		this.sleep(10);

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		this.sleep(10);

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		this.sleep(10);

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		this.sleep(10);

		long afterFifthCommit = tx.getTimestamp();

		tx.put("6", "Six");
		tx.commit("Sixth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterFifthCommit);
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);

		long requestTimestamp = (afterFourthCommit + afterThirdCommit) / 2;

		List<Long> commitsAround = tx.getCommitTimestampsAround(requestTimestamp, 3);
		assertEquals(expected, commitsAround);
	}

	// =================================================================================================================
	// COMMITS BEFORE
	// =================================================================================================================

	@Test
	public void commitsBeforeWorksIfThereAreNoCommits() {
		ChronoDB db = this.getChronoDB();
		List<Entry<Long, Object>> commits = db.tx().getCommitMetadataBefore(0, 10);
		assertNotNull(commits);
		assertTrue(commits.isEmpty());
	}

	@Test
	public void commitsBeforeWorksIfThereAreNotEnoughCommits() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		tx.put("6", "Six");
		tx.commit("Sixth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterSecondCommit, "Second"));
		expected.add(Pair.of(afterFirstCommit, "First"));

		List<Entry<Long, Object>> commits = db.tx().getCommitMetadataBefore(afterThirdCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitsBeforeWorksIfThereAreExactlyEnoughCommits() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		tx.put("6", "Six");
		tx.commit("Sixth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));
		expected.add(Pair.of(afterFirstCommit, "First"));

		List<Entry<Long, Object>> commits = db.tx().getCommitMetadataBefore(afterFourthCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitsBeforeWorksIfThereAreTooManyCommits() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		long afterFifthCommit = tx.getTimestamp();

		tx.put("6", "Six");
		tx.commit("Sixth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));

		List<Entry<Long, Object>> commits = db.tx().getCommitMetadataBefore(afterFifthCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitTimestampsBeforeWorksIfThereAreNoCommits() {
		ChronoDB db = this.getChronoDB();
		List<Long> commits = db.tx().getCommitTimestampsBefore(0, 10);
		assertNotNull(commits);
		assertTrue(commits.isEmpty());
	}

	@Test
	public void commitTimestampsBeforeWorksIfThereAreNotEnoughCommits() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		tx.put("6", "Six");
		tx.commit("Sixth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterSecondCommit);
		expected.add(afterFirstCommit);

		List<Long> commits = db.tx().getCommitTimestampsBefore(afterThirdCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitTimestampsBeforeWorksIfThereAreExactlyEnoughCommits() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		tx.put("6", "Six");
		tx.commit("Sixth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);
		expected.add(afterFirstCommit);

		List<Long> commits = db.tx().getCommitTimestampsBefore(afterFourthCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitTimestampsBeforeWorksIfThereAreTooManyCommits() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		long afterFifthCommit = tx.getTimestamp();

		tx.put("6", "Six");
		tx.commit("Sixth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);

		List<Long> commits = db.tx().getCommitTimestampsBefore(afterFifthCommit, 3);

		assertEquals(expected, commits);
	}

	// =================================================================================================================
	// COMMITS AFTER
	// =================================================================================================================

	@Test
	public void commitsAfterWorksIfThereAreNoCommits() {
		ChronoDB db = this.getChronoDB();
		List<Entry<Long, Object>> commits = db.tx().getCommitMetadataAfter(0, 10);
		assertNotNull(commits);
		assertTrue(commits.isEmpty());
	}

	@Test
	public void commitsAfterWorksIfThereAreNotEnoughCommits() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));

		List<Entry<Long, Object>> commits = db.tx().getCommitMetadataAfter(afterFirstCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitsAfterWorksIfThereAreExactlyEnoughCommits() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));

		List<Entry<Long, Object>> commits = db.tx().getCommitMetadataAfter(afterFirstCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitsAfterWorksIfThereAreTooManyCommits() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		tx.put("6", "Six");
		tx.commit("Sixth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));

		List<Entry<Long, Object>> commits = db.tx().getCommitMetadataAfter(afterFirstCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitTimestampsAfterWorksIfThereAreNoCommits() {
		ChronoDB db = this.getChronoDB();
		List<Long> commits = db.tx().getCommitTimestampsAfter(0, 10);
		assertNotNull(commits);
		assertTrue(commits.isEmpty());
	}

	@Test
	public void commitTimestampsAfterWorksIfThereAreNotEnoughCommits() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		List<Long> expected = Lists.newArrayList();
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);

		List<Long> commits = db.tx().getCommitTimestampsAfter(afterFirstCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitTimestampsAfterWorksIfThereAreExactlyEnoughCommits() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		List<Long> expected = Lists.newArrayList();
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);

		List<Long> commits = db.tx().getCommitTimestampsAfter(afterFirstCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitTimestampsAfterWorksIfThereAreTooManyCommits() {
		ChronoDB db = this.getChronoDB();
		ChronoDBTransaction tx = db.tx();
		tx.put("Hello", "World");
		tx.commit("First");

		long afterFirstCommit = tx.getTimestamp();

		tx.put("Foo", "Bar");
		tx.commit("Second");

		long afterSecondCommit = tx.getTimestamp();

		tx.put("Pi", "3.1415");
		tx.commit("Third");

		long afterThirdCommit = tx.getTimestamp();

		tx.put("Test", "Test");
		tx.commit("Fourth");

		long afterFourthCommit = tx.getTimestamp();

		tx.put("XYZ", "XYZ");
		tx.commit("Fifth");

		tx.put("6", "Six");
		tx.commit("Sixth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);

		List<Long> commits = db.tx().getCommitTimestampsAfter(afterFirstCommit, 3);

		assertEquals(expected, commits);
	}

}
