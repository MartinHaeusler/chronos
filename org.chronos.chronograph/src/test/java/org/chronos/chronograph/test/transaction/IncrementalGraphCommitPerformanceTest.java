package org.chronos.chronograph.test.transaction;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplUtils;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronodb.test.base.AllBackendsTest.DontRunWithBackend;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronodb.test.util.TestUtils;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

@Category(PerformanceTest.class)
@DontRunWithBackend({ ChronosBackend.JDBC, ChronosBackend.INMEMORY, ChronosBackend.MAPDB })
public class IncrementalGraphCommitPerformanceTest extends AllChronoGraphBackendsTest {

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "200000")
	public void t_largeStructureTest() {
		final int MAX_BATCH_SIZE = 25_000;
		ChronoGraph graph = this.getGraph();
		// create indices
		graph.getIndexManager().create().stringIndex().onVertexProperty("kind").build();
		// simulate one entity class
		Vertex classVertex = graph.addVertex("kind", "class");
		graph.tx().commit();
		// load largeStructure files
		Scanner entitiesScanner = new Scanner(this.getClass().getClassLoader()
				.getResourceAsStream("org/chronos/chronograph/testStructures/entities.csv"));
		Scanner associationsScanner = new Scanner(this.getClass().getClassLoader()
				.getResourceAsStream("org/chronos/chronograph/testStructures/associations.csv"));
		// put entities
		int batchSize = 0;
		while (entitiesScanner.hasNextLine()) {
			String line = entitiesScanner.nextLine().trim();
			Vertex entityVertex = graph.addVertex(T.id, line, "kind", "entity");
			entityVertex.addEdge("classifier", classVertex);
			batchSize++;
			if (batchSize > MAX_BATCH_SIZE) {
				graph.tx().commitIncremental();
				batchSize = 0;
			}
		}
		graph.tx().commitIncremental();
		// put associations
		batchSize = 0;
		while (associationsScanner.hasNextLine()) {
			String line = associationsScanner.nextLine().trim();
			String[] sourceTarget = line.split(",");
			String source = sourceTarget[0].trim();
			String target = sourceTarget[1].trim();
			Vertex v1 = Iterators.getOnlyElement(graph.vertices(source), null);
			Vertex v2 = Iterators.getOnlyElement(graph.vertices(target), null);
			assertNotNull(v1);
			assertNotNull(v2);
			v1.addEdge("connected", v2);
			batchSize++;
			if (batchSize > MAX_BATCH_SIZE) {
				graph.tx().commitIncremental();
				batchSize = 0;
			}
		}
		// commit and free scanners
		graph.tx().commit();
		entitiesScanner.close();
		associationsScanner.close();
	}

	@Test
	public void massiveIncrementalCommitsProduceConsistentStoreWithBatchInsert() {
		this.runMassiveIncrementalCommitTest(true);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10000")
	public void massiveIncrementalCommitsProduceConsistentStoreWithBatchInsertAndCache() {
		this.runMassiveIncrementalCommitTest(true);
	}

	@Test
	public void massiveIncrementalCommitsProduceConsistentStoreWithRegularInsert() {
		this.runMassiveIncrementalCommitTest(false);
	}

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10000")
	public void massiveIncrementalCommitsProduceConsistentStoreWithRegularInsertAndCache() {
		this.runMassiveIncrementalCommitTest(false);
	}

	private void runMassiveIncrementalCommitTest(final boolean useBatch) {
		ChronoGraph graph = this.getGraph();

		// we want at least three batches
		final int keyCount = TuplUtils.BATCH_INSERT_THRESHOLD * 4;

		final int additionalEdgeCount = keyCount * 3;

		List<String> keysList = Lists.newArrayList();
		for (int i = 0; i < keyCount; i++) {
			keysList.add(UUID.randomUUID().toString());
		}
		keysList = Collections.unmodifiableList(keysList);

		final int maxBatchSize;
		if (useBatch) {
			// we force batch inserts by choosing a size larger than the batch threshold
			maxBatchSize = TuplUtils.BATCH_INSERT_THRESHOLD + 1;
		} else {
			// we force normal inserts by choosing a size less than the batch threshold
			maxBatchSize = TuplUtils.BATCH_INSERT_THRESHOLD - 1;
		}

		graph.tx().open();

		int index = 0;
		int batchSize = 0;
		int batchCount = 0;
		List<String> addedVertexIds = Lists.newArrayList();
		while (index < keyCount) {
			String uuid = keysList.get(index);
			Vertex newVertex = graph.addVertex(T.id, uuid);
			if (addedVertexIds.size() > 1) {
				// connect to a random vertex (other than self; we add this vertex later)
				String randomVertexId = TestUtils.getRandomEntryOf(addedVertexIds);
				Vertex randomVertex = Iterators.getOnlyElement(graph.vertices(randomVertexId));
				newVertex.addEdge("connected", randomVertex);
			}
			addedVertexIds.add(uuid);
			index++;
			batchSize++;
			if (batchSize >= maxBatchSize) {
				for (int i = 0; i < index; i++) {
					String test = keysList.get(i);
					try {
						Vertex vertex = Iterators.getOnlyElement(graph.vertices(test));
						assertNotNull(vertex);
						assertEquals(test, vertex.id());
						if (i >= 2) {
							assertEquals(1, Iterators.size(vertex.edges(Direction.OUT)));
						}
					} catch (AssertionError e) {
						System.out.println("Error occurred on Test\t\tBatch: " + batchCount + "\t\ti: " + i
								+ "\t\tmaxIndex: " + index);
						throw e;
					}
				}
				graph.tx().commitIncremental();
				batchSize = 0;
				batchCount++;
				for (int i = 0; i < index; i++) {
					String test = keysList.get(i);
					try {
						Vertex vertex = Iterators.getOnlyElement(graph.vertices(test));
						assertNotNull(vertex);
						assertEquals(test, vertex.id());
						if (i >= 2) {
							assertEquals(1, Iterators.size(vertex.edges(Direction.OUT)));
						}
					} catch (AssertionError e) {
						System.out.println("Error occurred on Test\t\tBatch: " + batchCount + "\t\ti: " + i
								+ "\t\tmaxIndex: " + index);
						throw e;
					}
				}
			}
		}
		// now, do some linking
		batchSize = 0;
		batchCount = 0;
		for (int i = 0; i < additionalEdgeCount; i++) {
			String vId1 = TestUtils.getRandomEntryOf(keysList);
			String vId2 = TestUtils.getRandomEntryOf(keysList);
			Vertex v1 = Iterators.getOnlyElement(graph.vertices(vId1));
			Vertex v2 = Iterators.getOnlyElement(graph.vertices(vId2));
			v1.addEdge("additional", v2);
			batchSize++;
			if (batchSize >= maxBatchSize) {
				batchSize = 0;
				batchCount++;
				graph.tx().commitIncremental();
			}
		}

		graph.tx().commit();
		graph.tx().open();
		// check that all elements are present in the old transaction
		int i = 0;
		for (String uuid : keysList) {
			Vertex vertex = Iterators.getOnlyElement(graph.vertices(uuid));
			assertNotNull(vertex);
			assertEquals(uuid, vertex.id());
			if (i >= 2) {
				assertEquals(1, Iterators.size(vertex.edges(Direction.OUT, "connected")));
			}
			i++;
		}
		// check that all edges are there
		assertEquals(additionalEdgeCount,
				Iterators.size(graph.traversal().E().filter(t -> t.get().label().equals("additional"))));
		graph.tx().rollback();
	}

}
