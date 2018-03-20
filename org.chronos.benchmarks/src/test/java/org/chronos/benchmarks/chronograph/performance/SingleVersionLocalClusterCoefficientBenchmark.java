package org.chronos.benchmarks.chronograph.performance;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.common.test.utils.Statistic;
import org.chronos.common.test.utils.TimeStatistics;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class SingleVersionLocalClusterCoefficientBenchmark {

	@Test
	public void runTestChunkDB() {
		this.runTestAndPrintStatistics(dir -> {
			File graphFile = new File(dir, "test.chronos");
			Configuration configuration = new BaseConfiguration();
			configuration.setProperty("org.chronos.chronodb.storage.backend", "CHUNKED");
			configuration.setProperty("org.chronos.chronodb.storage.file.work_directory", graphFile.getAbsolutePath());
			configuration.setProperty("org.chronos.chronodb.cache.enabled", "true");
			configuration.setProperty("org.chronos.chronodb.cache.maxSize", 100_000);
			configuration.setProperty("org.chronos.chronodb.cache.assumeValuesAreImmutable", "true");
			configuration.setProperty("org.chronos.chronodb.temporal.enableBlindOverwriteProtection", "false");
			configuration.setProperty("org.chronos.chronodb.temporal.duplicateVersionEliminationMode", "false");
			configuration.setProperty("org.chronos.chronograph.transaction.autoOpen", "true");
			configuration.setProperty("org.chronos.chronograph.transaction.checkIdExistenceOnAdd", "false");
			return ChronoGraph.FACTORY.create().fromConfiguration(configuration).build();
		});
	}

	private void runTestAndPrintStatistics(final Function<File, Graph> graphFactory) {
		Statistic loadStatistics = new Statistic();
		Statistic calculationStatistics = new Statistic();
		for (int i = 0; i < 10; i++) {
			System.out.println();
			System.out.println("================================================================================");
			System.out.println(" RUN #" + i + "                                                                 ");
			System.out.println("================================================================================");
			System.out.println();
			Pair<Double, Double> result = runTest(graphFactory, 2);
			double loadTime = result.getLeft();
			double calculationTime = result.getRight();
			loadStatistics.addSample(loadTime);
			calculationStatistics.addSample(calculationTime);
			// try {
			// System.out.println("Sleeping for an hour... zzzzzzzZZZZzzzzZZZzzz");
			// Thread.sleep(1000 * 60 * 60);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
			System.gc();
			System.gc();
			System.gc();
		}
		System.out.println();
		System.out.println("================================================================================");
		System.out.println(" RESULTS                                                                        ");
		System.out.println("================================================================================");
		System.out.println();
		System.out.println("LOAD STATISTICS");
		TimeStatistics loadStats = new TimeStatistics(loadStatistics);
		System.out.println(loadStats.toFullString());
		System.out.println("CALCULATION STATISTICS");
		TimeStatistics calcStats = new TimeStatistics(calculationStatistics);
		System.out.println(calcStats.toFullString());
	}

	private static Pair<Double, Double> runTest(final Function<File, Graph> graphFactory, final int searchDepth) {
		File tempDir = Files.createTempDir();
		try {
			Graph g = graphFactory.apply(tempDir);
			System.out.println("Loading graph elements...");
			long beforeGraphLoad = System.currentTimeMillis();
			Map<String, Vertex> idToVertexMap = loadGraphElements(g);
			long afterGraphLoad = System.currentTimeMillis();
			System.out.println("Loaded graph elements in " + (afterGraphLoad - beforeGraphLoad) + "ms.");
			System.out.println("Loading random vertex IDs...");
			List<String> vertexIds = loadRandomVertexIds();
			System.out.println("Loaded " + vertexIds.size() + " random vertex IDs.");
			// System.out.println(
			// "About to calculate the cluster coefficients. Attach the profiler now. Waiting for 15 seconds.");
			// Thread.sleep(15_000);
			System.out.println("Starting calculation of cluster coefficients.");
			double sum = 0;
			long beforeClusterCalculation = System.currentTimeMillis();
			for (String vertexId : vertexIds) {
				Vertex vertex = g.vertices(idToVertexMap.get(vertexId)).next();
				sum += undirectedLocalClusterCoefficient(g, vertex, searchDepth);
			}
			long afterClusterCalculation = System.currentTimeMillis();
			g.close();
			System.out.println("Sum of all cluster coefficients: " + sum);
			System.out.println("Time taken: " + (afterClusterCalculation - beforeClusterCalculation) + "ms.");
			double loadingTime = afterGraphLoad - beforeGraphLoad;
			double calculationTime = afterClusterCalculation - beforeClusterCalculation;
			return Pair.of(loadingTime, calculationTime);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Test run failed", e);
		} finally {
			long sizeOfDirectory = FileUtils.sizeOfDirectory(tempDir);
			System.out.println("Working directory size: " + FileUtils.byteCountToDisplaySize(sizeOfDirectory));
			FileUtils.deleteQuietly(tempDir);
		}
	}

	private static Map<String, Vertex> loadGraphElements(final Graph g) {
		try {
			Map<String, Vertex> idToVertexMap = Maps.newHashMap();
			for (int i = 0; i < 100_000; i++) {
				idToVertexMap.put(String.valueOf(i), g.addVertex());
			}
			List<String> lines = FileUtils.readLines(new File(SingleVersionLocalClusterCoefficientBenchmark.class
					.getResource("/100kVertices300kEdgesGraphUniformlyRandomNoSelfEdges.txt").getFile()));
			for (String line : lines) {
				String[] split = line.split(";");
				String firstId = split[0];
				String secondId = split[1];
				Vertex vSource = idToVertexMap.get(firstId);
				Vertex vTarget = idToVertexMap.get(secondId);
				vSource.addEdge("c", vTarget);
			}
			g.tx().commit();
			return idToVertexMap;
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to load graph elements!", e);
		}
	}

	private static List<String> loadRandomVertexIds() {
		try {
			List<String> lines = FileUtils.readLines(new File(SingleVersionLocalClusterCoefficientBenchmark.class
					.getResource("/10kRandomVertexIdsFrom100kProfilingGraph.txt").getFile()));
			return lines;
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to load random vertex list!", e);
		}
	}

	private static double undirectedLocalClusterCoefficient(final Graph g, final Vertex vertex, final int depth) {
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
		for (int level = 0; level < depth; level++) {
			Set<Vertex> additionalNeighbors = Sets.newHashSet();
			for (Vertex v : neighborhood) {
				v.vertices(Direction.BOTH).forEachRemaining(neighbor -> additionalNeighbors.add(neighbor));
			}
			neighborhood.addAll(additionalNeighbors);
		}
		int neighborhoodSize = neighborhood.size();
		Set<Edge> interNeighborhoodEdges = Sets.newHashSet();
		for (Vertex v : neighborhood) {
			Iterator<Edge> outEdgesWithinNeighborhood = Iterators.filter(v.edges(Direction.OUT),
					edge -> neighborhood.contains(edge.inVertex()));
			Iterator<Edge> inEdgesWithinNeighborhood = Iterators.filter(v.edges(Direction.IN),
					edge -> neighborhood.contains(edge.outVertex()));
			outEdgesWithinNeighborhood.forEachRemaining(e -> interNeighborhoodEdges.add(e));
			inEdgesWithinNeighborhood.forEachRemaining(e -> interNeighborhoodEdges.add(e));
		}
		double clusterCoefficient = (double) 2 * interNeighborhoodEdges.size()
				/ (neighborhoodSize * (neighborhoodSize - 1));
		return clusterCoefficient;
	}
}