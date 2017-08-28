package org.chronos.chronograph.test.commitmetadata;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(IntegrationTest.class)
public class GetCommitsBeforeAfterAroundTest extends AllChronoGraphBackendsTest {

	// =================================================================================================================
	// COMMITS AROUND
	// =================================================================================================================

	@Test
	public void commitsAroundWorksIfNoCommitsArePresent() {
		ChronoGraph graph = this.getGraph();
		try (ChronoGraph txGraph = graph.tx().createThreadedTx()) {
			List<Entry<Long, Object>> commits = txGraph.getCommitMetadataAround(0, 10);
			assertNotNull(commits);
			assertTrue(commits.isEmpty());
		}
	}

	@Test
	public void commitsAroundWorksIfNotEnoughCommitsArePresentOnEitherSide() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		List<Entry<Long, Object>> commitsAround = graph.getCommitMetadataAround(afterSecondCommit, 5);
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
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		List<Entry<Long, Object>> commitsAround = graph.getCommitMetadataAround(afterSecondCommit, 3);
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
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));
		expected.add(Pair.of(afterFirstCommit, "First"));

		List<Entry<Long, Object>> commitsAround = graph.getCommitMetadataAround(afterSecondCommit, 4);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitsAroundWorksIfNotEnoughElementsArePresentAfter() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		long afterFifthCommit = graph.getNow();

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		long afterSixthCommit = graph.getNow();

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterSixthCommit, "Sixth"));
		expected.add(Pair.of(afterFifthCommit, "Fifth"));
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));

		List<Entry<Long, Object>> commitsAround = graph.getCommitMetadataAround(afterFifthCommit, 5);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitsAroundWorksIfMoreElementsThanNecessaryArePresentOnBothSides() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));

		List<Entry<Long, Object>> commitsAround = graph.getCommitMetadataAround(afterThirdCommit, 3);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitsAroundWorksIfTimestampIsNotExactlyMatchingACommit() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		this.sleep(10);

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		this.sleep(10);

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		this.sleep(10);

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		this.sleep(10);

		long afterFifthCommit = graph.getNow();

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterFifthCommit, "Fifth"));
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));

		long requestTimestamp = (afterFourthCommit + afterThirdCommit) / 2;

		List<Entry<Long, Object>> commitsAround = graph.getCommitMetadataAround(requestTimestamp, 3);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitTimestampsAroundWorksIfNotEnoughCommitsArePresentOnEitherSide() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		List<Long> commitsAround = graph.getCommitTimestampsAround(afterSecondCommit, 5);
		assertEquals(3, commitsAround.size());
		assertEquals(afterThirdCommit, (long) commitsAround.get(0));
		assertEquals(afterSecondCommit, (long) commitsAround.get(1));
		assertEquals(afterFirstCommit, (long) commitsAround.get(2));
	}

	@Test
	public void commitTimestampsAroundWorksIfExactlyEnoughElementsArePresentOnBothSides() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		List<Long> commitsAround = graph.getCommitTimestampsAround(afterSecondCommit, 3);
		assertEquals(3, commitsAround.size());
		assertEquals(afterThirdCommit, (long) commitsAround.get(0));
		assertEquals(afterSecondCommit, (long) commitsAround.get(1));
		assertEquals(afterFirstCommit, (long) commitsAround.get(2));
	}

	@Test
	public void commitTimestampsAroundWorksIfNotEnoughElementsArePresentBefore() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);
		expected.add(afterFirstCommit);

		List<Long> commitsAround = graph.getCommitTimestampsAround(afterSecondCommit, 4);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitTimestampsAroundWorksIfNotEnoughElementsArePresentAfter() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		long afterFifthCommit = graph.getNow();

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		long afterSixthCommit = graph.getNow();

		List<Long> expected = Lists.newArrayList();
		expected.add(afterSixthCommit);
		expected.add(afterFifthCommit);
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);

		List<Long> commitsAround = graph.getCommitTimestampsAround(afterFifthCommit, 5);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitTimestampsAroundWorksIfMoreElementsThanNecessaryArePresentOnBothSides() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);

		List<Long> commitsAround = graph.getCommitTimestampsAround(afterThirdCommit, 3);
		assertEquals(expected, commitsAround);
	}

	@Test
	public void commitTimestampsAroundWorksIfTimestampIsNotExactlyMatchingACommit() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		this.sleep(10);

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		this.sleep(10);

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		this.sleep(10);

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		this.sleep(10);

		long afterFifthCommit = graph.getNow();

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterFifthCommit);
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);

		long requestTimestamp = (afterFourthCommit + afterThirdCommit) / 2;

		List<Long> commitsAround = graph.getCommitTimestampsAround(requestTimestamp, 3);
		assertEquals(expected, commitsAround);
	}

	// =================================================================================================================
	// COMMITS BEFORE
	// =================================================================================================================

	@Test
	public void commitsBeforeWorksIfThereAreNoCommits() {
		ChronoGraph graph = this.getGraph();
		List<Entry<Long, Object>> commits = graph.getCommitMetadataBefore(0, 10);
		assertNotNull(commits);
		assertTrue(commits.isEmpty());
	}

	@Test
	public void commitsBeforeWorksIfThereAreNotEnoughCommits() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterSecondCommit, "Second"));
		expected.add(Pair.of(afterFirstCommit, "First"));

		List<Entry<Long, Object>> commits = graph.getCommitMetadataBefore(afterThirdCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitsBeforeWorksIfThereAreExactlyEnoughCommits() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));
		expected.add(Pair.of(afterFirstCommit, "First"));

		List<Entry<Long, Object>> commits = graph.getCommitMetadataBefore(afterFourthCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitsBeforeWorksIfThereAreTooManyCommits() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		long afterFifthCommit = graph.getNow();

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));

		List<Entry<Long, Object>> commits = graph.getCommitMetadataBefore(afterFifthCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitTimestampsBeforeWorksIfThereAreNoCommits() {
		ChronoGraph graph = this.getGraph();
		List<Long> commits = graph.getCommitTimestampsBefore(0, 10);
		assertNotNull(commits);
		assertTrue(commits.isEmpty());
	}

	@Test
	public void commitTimestampsBeforeWorksIfThereAreNotEnoughCommits() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterSecondCommit);
		expected.add(afterFirstCommit);

		List<Long> commits = graph.getCommitTimestampsBefore(afterThirdCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitTimestampsBeforeWorksIfThereAreExactlyEnoughCommits() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);
		expected.add(afterFirstCommit);

		List<Long> commits = graph.getCommitTimestampsBefore(afterFourthCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitTimestampsBeforeWorksIfThereAreTooManyCommits() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		long afterFifthCommit = graph.getNow();

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);

		List<Long> commits = graph.getCommitTimestampsBefore(afterFifthCommit, 3);

		assertEquals(expected, commits);
	}

	// =================================================================================================================
	// COMMITS AFTER
	// =================================================================================================================

	@Test
	public void commitsAfterWorksIfThereAreNoCommits() {
		ChronoGraph graph = this.getGraph();
		List<Entry<Long, Object>> commits = graph.getCommitMetadataAfter(0, 10);
		assertNotNull(commits);
		assertTrue(commits.isEmpty());
	}

	@Test
	public void commitsAfterWorksIfThereAreNotEnoughCommits() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));

		List<Entry<Long, Object>> commits = graph.getCommitMetadataAfter(afterFirstCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitsAfterWorksIfThereAreExactlyEnoughCommits() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));

		List<Entry<Long, Object>> commits = graph.getCommitMetadataAfter(afterFirstCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitsAfterWorksIfThereAreTooManyCommits() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		List<Entry<Long, Object>> expected = Lists.newArrayList();
		expected.add(Pair.of(afterFourthCommit, "Fourth"));
		expected.add(Pair.of(afterThirdCommit, "Third"));
		expected.add(Pair.of(afterSecondCommit, "Second"));

		List<Entry<Long, Object>> commits = graph.getCommitMetadataAfter(afterFirstCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitTimestampsAfterWorksIfThereAreNoCommits() {
		ChronoGraph graph = this.getGraph();
		List<Long> commits = graph.getCommitTimestampsAfter(0, 10);
		assertNotNull(commits);
		assertTrue(commits.isEmpty());
	}

	@Test
	public void commitTimestampsAfterWorksIfThereAreNotEnoughCommits() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		List<Long> expected = Lists.newArrayList();
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);

		List<Long> commits = graph.getCommitTimestampsAfter(afterFirstCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitTimestampsAfterWorksIfThereAreExactlyEnoughCommits() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		List<Long> expected = Lists.newArrayList();
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);

		List<Long> commits = graph.getCommitTimestampsAfter(afterFirstCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitTimestampsAfterWorksIfThereAreTooManyCommits() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		long afterFourthCommit = graph.getNow();

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterFourthCommit);
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);

		List<Long> commits = graph.getCommitTimestampsAfter(afterFirstCommit, 3);

		assertEquals(expected, commits);
	}

	@Test
	public void commitTimestampsAfterZeroWorks() {
		ChronoGraph graph = this.getGraph();
		graph.addVertex("Hello", "World");
		graph.tx().commit("First");

		long afterFirstCommit = graph.getNow();

		graph.addVertex("Foo", "Bar");
		graph.tx().commit("Second");

		long afterSecondCommit = graph.getNow();

		graph.addVertex("Pi", "3.1415");
		graph.tx().commit("Third");

		long afterThirdCommit = graph.getNow();

		graph.addVertex("Test", "Test");
		graph.tx().commit("Fourth");

		graph.addVertex("XYZ", "XYZ");
		graph.tx().commit("Fifth");

		graph.addVertex("6", "Six");
		graph.tx().commit("Sixth");

		List<Long> expected = Lists.newArrayList();
		expected.add(afterThirdCommit);
		expected.add(afterSecondCommit);
		expected.add(afterFirstCommit);

		List<Long> commits = graph.getCommitTimestampsAfter(0, 3);

		assertEquals(expected, commits);
	}

}
