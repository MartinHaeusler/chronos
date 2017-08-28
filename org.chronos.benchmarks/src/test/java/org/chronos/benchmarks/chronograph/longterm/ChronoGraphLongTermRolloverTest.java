package org.chronos.benchmarks.chronograph.longterm;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.benchmarks.chronograph.performance.IncreasingVersionCountReadWrite;
import org.chronos.benchmarks.util.BenchmarkUtils;
import org.chronos.benchmarks.util.statistics.ProbabilityDistribution;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.test.utils.TimeStatistics;
import org.chronos.common.util.ObjectSizeCalculator;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ChronoGraphLongTermRolloverTest {

	// =====================================================================================================================
	// CHRONODB SETTINGS
	// =====================================================================================================================

	/** The Chronos Backend to use for the benchmark. */
	private static final ChronosBackend BACKEND = ChronosBackend.CHUNKDB;
	/** Enables/Disables caching on ChronoDB level. */
	private static final boolean CACHE_ENABLED = true;
	/** The size of the ChronoDB cache, in bytes. */
	private static final long CACHE_SIZE_BYTES = 1073741824; // = 1 GB

	// =====================================================================================================================
	// BENCHMARK SETTINGS
	// =====================================================================================================================

	/** The number of iterations to execute before terminating the benchmark. */
	private static final int ITERATIONS = 1_000_000; // was 10
	/** The number of read batches executed per iteration. */
	private static final int READS_PER_ITERATION = 10;
	/** The number of vertices on which to calculate the local cluster coefficient per iteration. */
	private static final int VERTEX_IDS_TO_CALCULATE_CLUSTER_COEFFICIENT_FOR = 10_000;
	/** The number of commits performed per benchmark iteration. */
	private static final int COMMITS_PER_ITERATION = 300; // was 100
	/** The number of random graph modifications executed per commit. */
	private static final int MODIFICATIONS_PER_COMMIT = 50; // was 20

	/** The search depth for the local cluster coefficient. Higher depth means heavier load. */
	private static final int LOCAL_CLUSTER_COEFFICIENT__SEARCH_DEPTH = 2;

	/** The minimum required number of vertices in the graph at any point in time. */
	private static final int GRAPH__MIN_VERTEX_COUNT = 95_000;
	/** The maximum allowed number of vertices in the graph at any point in time. */
	private static final int GRAPH__MAX_VERTEX_COUNT = 105_000;
	/** The minimum required number of edges in the graph at any point in time. */
	private static final int GRAPH__MIN_EDGE_COUNT = 285_000;
	/** The maximum allowed number of edges in the graph at any point in time. */
	private static final int GRAPH__MAX_EDGE_COUNT = 315_000;

	/** Determines how to select the timestamp at which the read operation benchmark is executed. */
	private static final ReadTimestamp READ_TIMESTAMP_SELECTION = ReadTimestamp.RANDOM_IN_HEAD_CHUNK;

	/** Enables/disables rollovers. If enabled, only backends that support it actually perform rollovers. */
	private static final boolean ENABLE_ROLLOVERS = true;
	/** Determines after how many iteration a rollover is performed. Must not be negative. */
	private static final int ROLLOVER_AFTER_ITERATIONS = 20;

	/**
	 * Enables/disables resource-intensive calculation of cache size on each iteration. If false, only an estimate is
	 * printed.
	 */
	private static final boolean CALCULATE_CACHE_SIZE = false;

	// =====================================================================================================================
	// STATIC STUFF
	// =====================================================================================================================

	private static final Random RANDOM = new Random();

	// =====================================================================================================================
	// BENCHMARK MAIN METHOD
	// =====================================================================================================================

	@Test
	public void runBenchmark() {
		// create the random distribution from test settings
		System.out.println("Running Benchmark on backend [" + BACKEND + "]. Timestamp selection mode is: " + READ_TIMESTAMP_SELECTION);

		// instantiate the graph
		ChronoGraph graph = instantiateChronoGraph();

		// load the base graph
		GraphMetadata metadata = loadBaseGraphElementsFromFile(graph);

		// initialize the time range boundaries to the "now" timestamp of the graph
		final long timestampLowerBound = graph.getNow();
		long timestampUpperBound = graph.getNow();

		long headChunkLowerBound = timestampLowerBound;
		long headChunkUpperBound = timestampUpperBound;

		Map<Integer, List<TimeStatistics>> rolloverCountToQueryStatistics = Maps.newHashMap();
		TimeStatistics rolloverStatistics = new TimeStatistics();
		int rolloverCount = 0;

		List<TimeStatistics> currentQueryTimeStatistics = Lists.newArrayList();
		rolloverCountToQueryStatistics.put(rolloverCount, currentQueryTimeStatistics);

		// perform the individual iterations
		for (int iteration = 0; iteration < ITERATIONS; iteration++) {
			System.out.println("Iteration #" + (iteration + 1) + " of " + ITERATIONS);
			System.out.print("\tModification Phase. Performing " + COMMITS_PER_ITERATION + " commits (commit size: " + MODIFICATIONS_PER_COMMIT + " changes)   ");
			for (int commit = 0; commit < COMMITS_PER_ITERATION; commit++) {
				System.out.print(".");
				performRandomGraphMutations(graph, metadata);
			}
			System.out.println();
			System.out.println("\tGraph after modifications: V: " + metadata.getVertexCount() + ", E: " + metadata.getEdgeCount());
			// advance the upper bound of the time range
			timestampUpperBound = graph.getNow();
			headChunkUpperBound = timestampUpperBound;
			// initialize a time statistics object
			TimeStatistics statistics = new TimeStatistics();
			// start the reads
			System.out.println("\tRead Phase");
			double sum = 0;
			System.gc();
			System.gc();
			System.gc();
			for (int read = 0; read < READS_PER_ITERATION; read++) {
				long timestamp = selectTransactionTimestamp(timestampLowerBound, timestampUpperBound, headChunkLowerBound, headChunkUpperBound);
				System.out.println("\t\tRead #" + (read + 1) + " of " + READS_PER_ITERATION + " at timestamp [begin]+" + (timestamp - timestampLowerBound));
				List<String> vertexIdsAtTimestamp = Lists.newArrayList();
				graph.tx().open(timestamp);
				try {
					graph.vertices().forEachRemaining(v -> vertexIdsAtTimestamp.add((String) v.id()));
				} finally {
					graph.tx().rollback();
				}
				Collections.shuffle(vertexIdsAtTimestamp);
				List<String> chosenVertexIds = vertexIdsAtTimestamp.subList(0, VERTEX_IDS_TO_CALCULATE_CLUSTER_COEFFICIENT_FOR);
				if (chosenVertexIds.size() < VERTEX_IDS_TO_CALCULATE_CLUSTER_COEFFICIENT_FOR) {
					throw new IllegalStateException("Could not select " + VERTEX_IDS_TO_CALCULATE_CLUSTER_COEFFICIENT_FOR + " random vertex IDs to calculate the local cluster coefficient for; " + "please reduce the weight on vertex deletions or increase the weight on vertex additions.");
				}
				statistics.beginRun();
				sum += performReads(graph, chosenVertexIds, timestamp);
				statistics.endRun();
			}
			System.out.println("\tSum: " + sum);
			System.out.println("\tRead Statistics: " + statistics.toCSV());
			currentQueryTimeStatistics.add(statistics);
			printStatistics(rolloverCountToQueryStatistics, rolloverStatistics);
			System.out.println("Current Change Event Distribution: " + calculateModificationDistribution(metadata).toString());
			ChronoDBCache cache = graph.getBackingDB().getCache();
			System.out.println("Cache Statistics: " + cache.getStatistics().toString());
			System.out.println("Cache Size (Element Count):  " + cache.size());
			if (CALCULATE_CACHE_SIZE) {
				System.out.println("Cache Size (Calculated): " + FileUtils.byteCountToDisplaySize(ObjectSizeCalculator.getObjectSize(cache)));
			} else {
				System.out.println("Cache Size (Calculated): <CALCULATION DISABLED>");
			}
			if (ENABLE_ROLLOVERS && iteration > 0 && iteration % ROLLOVER_AFTER_ITERATIONS == 0) {
				// perform a rollover
				rolloverStatistics.beginRun();
				graph.getBackingDB().getMaintenanceManager().performRolloverOnAllBranches();
				rolloverStatistics.endRun();
				System.out.println("PERFORMED ROLLOVER.");
				rolloverCount++;
				currentQueryTimeStatistics = Lists.newArrayList();
				rolloverCountToQueryStatistics.put(rolloverCount, currentQueryTimeStatistics);
				headChunkLowerBound = graph.getNow();
				headChunkUpperBound = graph.getNow();
			}
		}
		System.out.println("=============================================================");
		System.out.println("                    BENCHMARK RUN END                        ");
		System.out.println("=============================================================");
		System.out.println();
		printStatistics(rolloverCountToQueryStatistics, rolloverStatistics);
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private static ChronoGraph instantiateChronoGraph() {
		try {
			File tempDir = java.nio.file.Files.createTempDirectory("ChronoGraphTest").toFile();
			tempDir.deleteOnExit();
			File dbRoot = new File(tempDir, "test.chronos");
			System.out.println("Graph is stored here: " + dbRoot.getAbsolutePath());
			dbRoot.createNewFile();
			Configuration config = new BaseConfiguration();
			config.setProperty("org.chronos.chronodb.storage.backend", BACKEND.toString());
			config.setProperty("org.chronos.chronodb.storage.file.work_directory", dbRoot.getAbsolutePath());
			config.setProperty("org.chronos.chronodb.temporal.duplicateVersionEliminationMode", "off");
			config.setProperty("org.chronos.chronodb.temporal.enableBlindOverwriteProtection", "false");
			config.setProperty("org.chronos.chronodb.cache.assumeValuesAreImmutable", "true");
			config.setProperty("org.chronos.chronodb.cache.enabled", String.valueOf(CACHE_ENABLED));
			config.setProperty("org.chronos.chronodb.cache.maxSize", String.valueOf(CACHE_SIZE_BYTES));
			config.setProperty("org.chronos.chronograph.transaction.checkIdExistenceOnAdd", "false");
			config.setProperty("org.chronos.chronograph.transaction.autoOpen", "true");
			return ChronoGraph.FACTORY.create().fromConfiguration(config).build();
		} catch (IOException e) {
			throw new RuntimeException("Failed to create temp directory!", e);
		}
	}

	private static GraphMetadata loadBaseGraphElementsFromFile(final Graph g) {
		try {
			Set<String> vertexIds = Sets.newHashSet();
			List<String> lines = FileUtils.readLines(new File(IncreasingVersionCountReadWrite.class.getResource("/100kVertices300kEdgesGraphUniformlyRandomNoSelfEdges.txt").getFile()));
			int edgeCount = 0;
			for (String line : lines) {
				String[] split = line.split(";");
				String firstId = split[0];
				String secondId = split[1];
				Vertex vSource = null;
				Vertex vTarget = null;
				if (g.vertices(firstId).hasNext()) {
					vSource = g.vertices(firstId).next();
				} else {
					vSource = g.addVertex(T.id, firstId);
					vertexIds.add(firstId);
				}
				if (g.vertices(secondId).hasNext()) {
					vTarget = g.vertices(secondId).next();
				} else {
					vTarget = g.addVertex(T.id, secondId);
					vertexIds.add(secondId);
				}
				vSource.addEdge("c", vTarget);
				edgeCount++;
			}
			g.tx().commit();
			GraphMetadata metadata = new GraphMetadata();
			metadata.setVertexIds(vertexIds);
			metadata.setEdgeCount(edgeCount);
			return metadata;
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to load graph elements!", e);
		}
	}

	private static void performRandomGraphMutations(final Graph g, final GraphMetadata metadata) {
		try {
			g.tx().open();
			for (int modCount = 0; modCount < MODIFICATIONS_PER_COMMIT; modCount++) {
				// recalculate the probability distribution based on the current graph contents
				ProbabilityDistribution<Modification> distribution = calculateModificationDistribution(metadata);
				// draw the next event from the distribution
				Modification modification = distribution.nextEvent();
				switch (modification) {
				case ADD_EDGE:
					executeModificationAddEdge(g, metadata);
					break;
				case ADD_VERTEX:
					executeModificationAddVertex(g, metadata);
					break;
				case REMOVE_EDGE:
					executeModificationRemoveEdge(g, metadata);
					break;
				case REMOVE_VERTEX:
					executeModificationRemoveVertex(g, metadata);
					break;
				default:
					throw new UnknownEnumLiteralException(modification);
				}
			}
			g.tx().commit();
		} finally {
			g.tx().rollback();
		}
	}

	private static long selectTransactionTimestamp(final long min, final long max, final long headChunkMin, final long headChunkMax) {
		switch (READ_TIMESTAMP_SELECTION) {
		case ALWAYS_HALFWAY:
			return min + (max - min) / 2;
		case ALWAYS_HEAD:
			return max;
		case ALWAYS_INITIAL:
			return min;
		case RANDOM_IN_RANGE:
			return BenchmarkUtils.randomBetween(min, max);
		case RANDOM_IN_HEAD_CHUNK:
			return BenchmarkUtils.randomBetween(headChunkMin, headChunkMax);
		default:
			throw new UnknownEnumLiteralException(READ_TIMESTAMP_SELECTION);
		}
	}

	private static ProbabilityDistribution<Modification> calculateModificationDistribution(final GraphMetadata metadata) {
		int vertexCount = metadata.getVertexCount();
		int edgeCount = metadata.getEdgeCount();
		int weightOfAddVertex = GRAPH__MAX_VERTEX_COUNT - vertexCount;
		int weightOfRemoveVertex = vertexCount - GRAPH__MIN_VERTEX_COUNT;
		int weightOfAddEdge = GRAPH__MAX_EDGE_COUNT - edgeCount;
		int weightOfRemoveEdge = edgeCount - GRAPH__MIN_EDGE_COUNT;
		// note: any of the weights above may become zero or negative. Those values are filtered out
		// by the distribution builder.
		ProbabilityDistribution<Modification> distribution = ProbabilityDistribution
				// create a discrete distribution
				.<Modification> discrete()
				//
				.event(Modification.ADD_VERTEX, weightOfAddVertex)
				//
				.event(Modification.REMOVE_VERTEX, weightOfRemoveVertex)
				//
				.event(Modification.ADD_EDGE, weightOfAddEdge)
				//
				.event(Modification.REMOVE_EDGE, weightOfRemoveEdge)
				// use our random generator to avoid creating thousands of Random instances
				.withRandomGenerator(RANDOM)
				//
				.build();
		// return it
		return distribution;
	}

	private static void executeModificationAddVertex(final Graph g, final GraphMetadata metadata) {
		// add a new vertex
		Vertex newVertex = g.addVertex();
		// connect it to a random other vertex in the graph
		Vertex existingVertex = g.vertices(metadata.getRandomVertexId()).next();
		newVertex.addEdge("c", existingVertex);
		// add the new vertex id to the metadata
		metadata.addVertexId((String) newVertex.id());
		// we also have one edge more than before
		metadata.setEdgeCount(metadata.getEdgeCount() + 1);
	}

	private static void executeModificationAddEdge(final Graph g, final GraphMetadata metadata) {
		boolean done = false;
		while (done == false) {
			// pick two random (distinct) vertices from the graph and add an edge between them
			String vertexId1 = metadata.getRandomVertexId();
			String vertexId2 = null;
			while (vertexId2 == null || vertexId2.equals(vertexId1)) {
				vertexId2 = metadata.getRandomVertexId();
			}
			Vertex v1 = g.vertices(vertexId1).next();
			Vertex v2 = g.vertices(vertexId2).next();
			// don't add the same edge twice
			if (Iterators.contains(v1.vertices(Direction.OUT, "c"), v2) == false) {
				v1.addEdge("c", v2);
				done = true;
			}
		}
		// we have one edge more than before
		metadata.setEdgeCount(metadata.getEdgeCount() + 1);
	}

	private static void executeModificationRemoveVertex(final Graph g, final GraphMetadata metadata) {
		// select a random vertex and remove it
		Vertex existingVertex = g.vertices(metadata.getRandomVertexId()).next();
		String vertexId = (String) existingVertex.id();
		// by deleting the vertex, we lose all connected edges. We need to count them first to keep
		// track of the overall edge count.
		int connectedEdges = Iterators.size(existingVertex.edges(Direction.BOTH, "c"));
		// delete the vertex
		existingVertex.remove();
		// update the graph metadata to reflect the change
		metadata.removeVertexId(vertexId);
		metadata.setEdgeCount(metadata.getEdgeCount() - connectedEdges);
	}

	private static void executeModificationRemoveEdge(final Graph g, final GraphMetadata metadata) {
		// choose a random edge and delete it; start by selecting a random vertex that has edges
		boolean done = false;
		while (done == false) {
			// select a random vertex
			Vertex existingVertex = g.vertices(metadata.getRandomVertexId()).next();
			// check if it has at least one edge
			if (existingVertex.edges(Direction.BOTH, "c").hasNext() == false) {
				// picked a vertex with no edges...
				continue;
			}
			// select a random edge from this vertex and delete it
			existingVertex.edges(Direction.BOTH, "c").next().remove();
			done = true;
		}
		// update the metadata
		metadata.setEdgeCount(metadata.getEdgeCount() - 1);
	}

	private static double performReads(final ChronoGraph graph, final List<String> vertexIds, final long timestamp) {
		double sum = 0;
		graph.tx().open(timestamp);
		try {
			for (String vertexId : vertexIds) {
				Vertex vertex = graph.vertices(vertexId).next();
				sum += undirectedLocalClusterCoefficient(vertex);
			}
		} finally {
			graph.tx().rollback();
		}
		return sum;
	}

	private static double undirectedLocalClusterCoefficient(final Vertex vertex) {
		Iterator<Edge> edges = vertex.edges(Direction.BOTH);
		if (edges.hasNext() == false) {
			// isolated vertices, and vertices with only one connected edge, have a
			// defined local cluster coefficient of zero.
			return 0;
		}
		edges.next();
		if (edges.hasNext() == false) {
			// isolated vertices, and vertices with only one connected edge, have a
			// defined local cluster coefficient of zero.
			return 0;
		}
		// calculate the neighborhood
		Set<Vertex> neighborhood = Sets.newHashSet();
		neighborhood.add(vertex);
		for (int level = 0; level < LOCAL_CLUSTER_COEFFICIENT__SEARCH_DEPTH; level++) {
			Set<Vertex> additionalNeighbors = Sets.newHashSet();
			for (Vertex v : neighborhood) {
				v.vertices(Direction.BOTH).forEachRemaining(neighbor -> additionalNeighbors.add(neighbor));
			}
			neighborhood.addAll(additionalNeighbors);
		}
		int neighborhoodSize = neighborhood.size();
		Set<Edge> interNeighborhoodEdges = Sets.newHashSet();
		for (Vertex v : neighborhood) {
			Iterator<Edge> outEdgesWithinNeighborhood = Iterators.filter(v.edges(Direction.OUT), edge -> neighborhood.contains(edge.inVertex()));
			Iterator<Edge> inEdgesWithinNeighborhood = Iterators.filter(v.edges(Direction.IN), edge -> neighborhood.contains(edge.outVertex()));
			outEdgesWithinNeighborhood.forEachRemaining(e -> interNeighborhoodEdges.add(e));
			inEdgesWithinNeighborhood.forEachRemaining(e -> interNeighborhoodEdges.add(e));
		}
		double clusterCoefficient = (double) 2 * interNeighborhoodEdges.size() / (neighborhoodSize * (neighborhoodSize - 1));
		return clusterCoefficient;
	}

	private static void printStatistics(final Map<Integer, List<TimeStatistics>> rolloverCountToQueryStats, final TimeStatistics rolloverStats) {
		System.out.println();
		System.out.println();
		System.out.println("=============================================================");
		System.out.println("                    STATISTICS REPORT                        ");
		System.out.println("=============================================================");
		System.out.println();
		System.out.println("ROLLOVER STATISTICS");
		if (rolloverStats.getRuntimes().isEmpty()) {
			System.out.println("< NO DATA >");
		} else {
			System.out.println("Summary: " + rolloverStats.toFullString());
			System.out.println("Individual rollover times:");
			{
				int rolloverIndex = 1;
				for (Double rollovertime : rolloverStats.getRuntimes()) {
					System.out.println("Rollover #" + rolloverIndex + ": " + String.format("%3.2f", rollovertime) + "ms");
					rolloverIndex++;
				}
			}
		}
		System.out.println();
		System.out.println("QUERY PERFORMANCE STATISTICS PER ROLLOVER");
		{
			for (int i = 0; i < rolloverCountToQueryStats.size(); i++) {
				List<TimeStatistics> timeStatistics = rolloverCountToQueryStats.get(i);
				if (timeStatistics == null || timeStatistics.isEmpty()) {
					System.out.println("Rollover #" + i + ": <NO DATA>");
				} else {
					for (int j = 0; j < timeStatistics.size(); j++) {
						System.out.println("Rollover #" + i + ", Run #" + (j + 1) + ": " + timeStatistics.get(j).toCSV());
					}
				}
			}
		}
		System.out.println();
		System.out.println("JVM STATISTICS");
		System.out.println("Max Memory:   " + FileUtils.byteCountToDisplaySize(Runtime.getRuntime().maxMemory()));
		System.out.println("Total Memory: " + FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory()));
		System.out.println("Free Memory: " + FileUtils.byteCountToDisplaySize(Runtime.getRuntime().freeMemory()));
		long uptime = ManagementFactory.getRuntimeMXBean().getUptime();
		String uptimeHHMMSS = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(uptime), TimeUnit.MILLISECONDS.toMinutes(uptime) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(uptime)), TimeUnit.MILLISECONDS.toSeconds(uptime) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(uptime)));
		System.out.println("Uptime: " + uptimeHHMMSS);
		System.out.println();
		System.out.println("=============================================================");
		System.out.println("                  STATISTICS REPORT END                      ");
		System.out.println("=============================================================");
		System.out.println();
		System.out.println();
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	private static enum Modification {

		ADD_VERTEX, REMOVE_VERTEX, ADD_EDGE, REMOVE_EDGE;

	}

	private static enum ReadTimestamp {

		RANDOM_IN_RANGE, RANDOM_IN_HEAD_CHUNK, ALWAYS_HEAD, ALWAYS_INITIAL, ALWAYS_HALFWAY;

	}

	private static class GraphMetadata {

		private final Set<String> vertexIdSet = Sets.newHashSet();
		private final List<String> vertexIdList = Lists.newArrayList();

		private int edgeCount;

		public void setVertexIds(final Collection<String> ids) {
			this.vertexIdSet.clear();
			this.vertexIdList.clear();
			if (ids != null) {
				this.vertexIdSet.addAll(ids);
				this.vertexIdList.addAll(this.vertexIdSet);
			}
		}

		public void addVertexId(final String vertexId) {
			checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
			boolean added = this.vertexIdSet.add(vertexId);
			if (added) {
				this.vertexIdList.add(vertexId);
			}
		}

		public void removeVertexId(final String vertexId) {
			checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
			boolean removed = this.vertexIdSet.remove(vertexId);
			if (removed) {
				this.vertexIdList.remove(vertexId);
			}
		}

		public int getVertexCount() {
			return this.vertexIdSet.size();
		}

		@SuppressWarnings("unused")
		public Set<String> getVertexIds() {
			return Collections.unmodifiableSet(this.vertexIdSet);
		}

		public String getRandomVertexId() {
			return BenchmarkUtils.getRandomEntryOf(this.vertexIdList);
		}

		public void setEdgeCount(final int edgeCount) {
			checkArgument(edgeCount >= 0, "Precondition violation - argument 'edgeCount' must not be negative!");
			this.edgeCount = edgeCount;
		}

		public int getEdgeCount() {
			return this.edgeCount;
		}

	}

}
