package org.chronos.benchmarks.chronograph.readwrite;

import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.benchmarks.util.BenchmarkUtils;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.utils.Measure;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(PerformanceTest.class)
public class TreeBuildingTest extends AllChronoGraphBackendsTest {

	private static final int NUMBER_OF_PERSONS = 100_000;
	private static final int NUMBER_OF_FRIENDS = 3;

	private static final String FIRST_NAME = "firstName";
	private static final String LAST_NAME = "lastName";
	private static final String BIRTHDATE = "birthDate";
	private static final String FRIEND = "friend";

	@Test
	public void runBenchmark() {
		ChronoGraph graph = this.getGraph();
		graph.tx().open();
		System.out.println("Generating graph data...");
		this.loadData(graph);
		System.out.println("Committing graph data...");
		graph.tx().commit();
		graph.tx().open();
		System.out.println("Data committed. Reading data...");
		Measure.startTimeMeasure("indexing");
		TreeMap<String, List<String>> firstNameIndex = new TreeMap<>();
		TreeMap<String, List<String>> lastNameIndex = new TreeMap<>();
		TreeMap<Long, List<String>> birthDateIndex = new TreeMap<>();
		graph.traversal().V().forEachRemaining(v -> {
			String id = (String) v.id();
			String firstName = v.value(FIRST_NAME);
			String lastName = v.value(LAST_NAME);
			long birthDate = v.value(BIRTHDATE);
			{ // update first name index
				List<String> firstNameList = firstNameIndex.get(firstName);
				if (firstNameList == null) {
					firstNameList = Lists.newArrayList();
					firstNameIndex.put(firstName, firstNameList);
				}
				firstNameList.add(id);
			}
			{ // update last name index
				List<String> lastNameList = lastNameIndex.get(lastName);
				if (lastNameList == null) {
					lastNameList = Lists.newArrayList();
					lastNameIndex.put(lastName, lastNameList);
				}
				lastNameList.add(id);
			}
			{ // update birth date index
				List<String> birthDateList = birthDateIndex.get(birthDate);
				if (birthDateList == null) {
					birthDateList = Lists.newArrayList();
					birthDateIndex.put(birthDate, birthDateList);
				}
				birthDateList.add(id);
			}
		});
		Measure.endTimeMeasure("indexing");
	}

	private void loadData(final ChronoGraph graph) {
		List<Vertex> persons = Lists.newArrayList();
		for (int i = 0; i < NUMBER_OF_PERSONS; i++) {
			Vertex person = graph.addVertex(T.id, UUID.randomUUID().toString());
			person.property(FIRST_NAME, UUID.randomUUID().toString());
			person.property(LAST_NAME, UUID.randomUUID().toString());
			person.property(BIRTHDATE, (long) Math.floor(Math.random() * Long.MAX_VALUE));
			persons.add(person);
		}
		// starting at half the persons, start connecting them
		for (Vertex person : persons) {
			for (int f = 0; f < NUMBER_OF_FRIENDS; f++) {
				Vertex friend = BenchmarkUtils.getRandomEntryOf(persons);
				person.addEdge(FRIEND, friend);
			}
		}
	}

}
