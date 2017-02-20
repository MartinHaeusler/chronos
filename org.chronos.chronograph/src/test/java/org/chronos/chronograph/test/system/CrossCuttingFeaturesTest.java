package org.chronos.chronograph.test.system;

import static org.junit.Assert.*;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.SystemTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@Category(SystemTest.class)
public class CrossCuttingFeaturesTest extends AllChronoGraphBackendsTest {

	private static final String P_FIRST_NAME = "firstname";
	private static final String P_LAST_NAME = "lastname";
	private static final String P_NICKNAMES = "nicknames";
	private static final String P_GENDER = "gender";
	private static final String E_KIND = "kind";

	private static final String SCENARIO_A = "Scenario A";
	private static final String SCENARIO_A1 = "Scenario A.1";

	@Test
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_AUTO_OPEN, value = "false")
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "10")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "10")
	public void phase1Works() {
		ChronoGraph graph = this.getGraph();
		runPhase1(graph);
	}

	@Test
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_AUTO_OPEN, value = "false")
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "10")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "10")
	public void phase2Works() {
		ChronoGraph graph = this.getGraph();
		runPhase1(graph);
		runPhase2(graph);
	}

	@Test
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_AUTO_OPEN, value = "false")
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "10")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "10")
	public void phase3Works() {
		ChronoGraph graph = this.getGraph();
		runPhase1(graph);
		runPhase2(graph);
		runPhase3(graph);
	}

	@Test
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_AUTO_OPEN, value = "false")
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "10")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "10")
	public void phase4Works() {
		ChronoGraph graph = this.getGraph();
		runPhase1(graph);
		runPhase2(graph);
		runPhase3(graph);
		runPhase4(graph);
	}

	// =====================================================================================================================
	// RUNNERS
	// =====================================================================================================================

	/**
	 * Performs phase 1 of the test.
	 *
	 * <p>
	 * This method will:
	 * <ul>
	 * <li>Create indices on {@link #P_FIRST_NAME}, {@link #P_LAST_NAME}, {@link #P_GENDER} and {@link #E_KIND}.
	 * <li>Create a base graph in a regular commit
	 * <li>Run queries on and off the indexer, in transient and persistent state
	 * </ul>
	 *
	 * <p>
	 * <b>INFORMAL SCENARIO DESCRIPTION:</b><br>
	 * We start off with two persons, John and Jack Doe, who are brothers.
	 *
	 * <p>
	 * <b>RESULTING GRAPH:</b>
	 * <ul>
	 * <li><b>v0:</b> First name: "John", Last name: "Doe", Nick names: ["JD", "Johnny"]
	 * <li><b>v1:</b> First name: "Jack", Last name: "Doe", Nick names: ["JD", "Jacky"]
	 * </ul>
	 * <ul>
	 * <li>John -[family; kind = brother]-> Jack
	 * <li>Jack -[family; kind = brother]-> John
	 * </ul>
	 *
	 * @param graph
	 *            The graph to execute the procedure on. Must not be <code>null</code>.
	 */
	private static void runPhase1(final ChronoGraph graph) {
		try {
			// create indices
			graph.getIndexManager().createIndex().onVertexProperty(P_FIRST_NAME).build();
			graph.getIndexManager().createIndex().onVertexProperty(P_LAST_NAME).build();
			graph.getIndexManager().createIndex().onEdgeProperty(E_KIND).build();
			graph.getIndexManager().createIndex().onVertexProperty(P_NICKNAMES).build();

			// create the base graph
			graph.tx().open();

			// add the base graph data
			Vertex v0 = graph.addVertex(T.id, "v0", P_FIRST_NAME, "John", P_LAST_NAME, "Doe", P_GENDER, "male");
			v0.property(P_NICKNAMES, Sets.newHashSet("JD", "Johnny"));
			Vertex v1 = graph.addVertex(T.id, "v1", P_FIRST_NAME, "Jack", P_LAST_NAME, "Doe", P_GENDER, "male");
			v1.property(P_NICKNAMES, Sets.newHashSet("JD", "Jacky"));
			v0.addEdge("family", v1, E_KIND, "brother");
			v1.addEdge("family", v0, E_KIND, "brother");

			assertCommitAssertCloseTx(graph,
					// assert function
					(final ChronoGraph g) -> {
						// there should be one john
						assertFirstNameCountEquals(graph, "John", 1);
						// there should be two does
						assertLastNameCountEquals(graph, "Doe", 2);
						// there should be 2 persons with a nickname that starts with "J"
						assertEquals(2, graph.find().vertices().where(P_NICKNAMES).startsWithIgnoreCase("j").count());
					} ,
					// commit function
					(final ChronoGraph g) -> {
						g.tx().commit();
						g.tx().open();
						return g;
					}
			// end of statement
			);
		} catch (Throwable t) {
			throw new AssertionError("Execution of Phase 1 failed! See root cause for details.", t);
		}
	}

	/**
	 * Performs phase 2 of the test.
	 *
	 * <p>
	 * This method will:
	 * <ul>
	 * <li>Expand the base graph with incremental commits (on the thread-local graph)
	 * <li>Run queries on and off the indexer, in transient and persistent state
	 * </ul>
	 *
	 * <p>
	 * <b>INFORMAL SCENARIO DESCRIPTION:</b><br>
	 * This scenario adds two women to the graph, Jane Smith and Jill Johnson. They are friends.
	 *
	 * <p>
	 * <b>RESULTING GRAPH:</b>
	 * <ul>
	 * <li><b>v0:</b> First name: "John", Last name: "Doe", Nick names: ["JD", "Johnny"]
	 * <li><b>v1:</b> First name: "Jack", Last name: "Doe", Nick names: ["JD", "Jacky"]
	 * <li><b>v2:</b> First name: "Jane", Last name: "Smith", Nick names: ["JS", "Jenny"]
	 * <li><b>v3:</b> First name: "Jill", Last name: "Johnson", Nick names: ["JJ", "JiJo"]
	 * </ul>
	 *
	 * <ul>
	 * <li>John -[family; kind = brother]-> Jack
	 * <li>Jack -[family; kind = brother]-> John
	 * <li>Jane -[knows; kind = friend]->Jill
	 * <li>Jill -[knows; kind = friend]->Jane
	 * </ul>
	 *
	 * @param graph
	 *            The graph to execute the procedure on. Must not be <code>null</code>.
	 */
	private static void runPhase2(final ChronoGraph graph) {
		try {
			// open a new graph transaction
			graph.tx().open();

			// extend the graph
			Vertex v2 = graph.addVertex(T.id, "v2", P_FIRST_NAME, "Jane", P_LAST_NAME, "Smith", P_GENDER, "female");
			v2.property(P_NICKNAMES, Sets.newHashSet("JS", "Jenny"));
			Vertex v3 = graph.addVertex(T.id, "v3", P_FIRST_NAME, "Jill", P_LAST_NAME, "Johnson", P_GENDER, "female");
			v3.property(P_NICKNAMES, Sets.newHashSet("JJ", "JiJo"));

			// there should be one john
			assertFirstNameCountEquals(graph, "John", 1);
			// we should have one Jane
			assertFirstNameCountEquals(graph, "Jane", 1);
			// there should be 4 persons with a nickname that starts with "J"
			assertEquals(4, graph.find().vertices().where(P_NICKNAMES).startsWithIgnoreCase("j").count());

			// perform the incremental commit
			graph.tx().commitIncremental();

			// there should be one john
			assertFirstNameCountEquals(graph, "John", 1);
			// we should have one Jane
			assertFirstNameCountEquals(graph, "Jane", 1);

			// create the associations
			v2.addEdge("knows", v3, E_KIND, "friend");
			v3.addEdge("knows", v2, E_KIND, "friend");

			assertCommitAssertCloseTx(graph,
					// assert function
					(final ChronoGraph g) -> {
						// jane should know jill
						assertEquals(1, g.traversal().V(v2).outE("knows").toSet().size());
						assertEquals(1, g.traversal().V(v3).inE("knows").toSet().size());
						// jill should know jane
						assertEquals(1, g.traversal().V(v3).outE("knows").toSet().size());
						assertEquals(1, g.traversal().V(v2).inE("knows").toSet().size());
						// there should be one john
						assertFirstNameCountEquals(g, "John", 1);
						// we should have one Jane
						assertFirstNameCountEquals(g, "Jane", 1);
						// jane should know jill
						assertEquals(1, g.traversal().V(v2).outE("knows").toSet().size());
						assertEquals(1, g.traversal().V(v3).inE("knows").toSet().size());
						// jill should know jane
						assertEquals(1, g.traversal().V(v3).outE("knows").toSet().size());
						assertEquals(1, g.traversal().V(v2).inE("knows").toSet().size());
					} ,
					// commit function
					(final ChronoGraph g) -> {
						g.tx().commit();
						g.tx().open();
						return g;
					}
			// end of statement
			);
		} catch (Throwable t) {
			throw new AssertionError("Execution of Phase 2 failed! See root cause for details.", t);
		}

	}

	/**
	 * Performs phase 3 of the test.
	 *
	 * <p>
	 * This method will:
	 * <ul>
	 * <li>Create a new branch named "Scenario A"
	 * <li>Expands the graph data in Scenario A by using threaded transactions
	 * </ul>
	 *
	 * <p>
	 * <b>INFORMAL SCENARIO DESCRIPTION:</b><br>
	 * John Doe marries Jane Smith. She takes his last name, becoming "Jane Doe". Of course, her prior nickname, "JS",
	 * no longer fits and is replaced by "JD". John and Jane have a daughter, Sarah Doe. Since Jane is married to John,
	 * Jane becomes Jack's Sister-in-Law. Jack is also the uncle of Sarah.
	 *
	 * <p>
	 * <b>RESULTING GRAPH:</b>
	 * <ul>
	 * <li><b>v0:</b> First name: "John", Last name: "Doe", Nick names: ["JD", "Johnny"]
	 * <li><b>v1:</b> First name: "Jack", Last name: "Doe", Nick names: ["JD", "Jacky"]
	 * <li><b>v2:</b> First name: "Jane", Last name: "Doe", Nick names: ["JD", "Jenny"]
	 * <li><b>v3:</b> First name: "Jill", Last name: "Johnson", Nick names: ["JJ", "JiJo"]
	 * <li><b>v4:</b> First name: "Sarah", Last name: "Doe", Nick names: ["SD", "Sassie"]
	 * </ul>
	 *
	 * <ul>
	 * <li>John -[family; kind = brother]-> Jack
	 * <li>Jack -[family; kind = brother]-> John
	 * <li>Jane -[knows; kind = friend]->Jill
	 * <li>Jill -[knows; kind = friend]->Jane
	 * <li>Jane -[family; kind = married]-> John
	 * <li>John -[family; kind = married]-> Jane
	 * <li>Jack -[family; kind = sister-in-law]-> Jane
	 * <li>Jane -[family; kind = brother-in-law]-> Jack
	 * <li>John -[family; kind = father]-> Sarah
	 * <li>Jane -[family; kind = mother]-> Sarah
	 * <li>Jack -[family; kind = uncle]-> Sarah
	 * </ul>
	 *
	 * @param graph
	 *            The graph to execute the procedure on. Must not be <code>null</code>.
	 */
	private static void runPhase3(final ChronoGraph graph) {
		try {
			// create the branch
			graph.getBranchManager().createBranch(SCENARIO_A);

			// create the threaded transaction graph
			ChronoGraph tx = graph.tx().createThreadedTx(SCENARIO_A);

			// fetch john and jane from the graph
			Vertex vJohn = tx.find().vertices().where(P_FIRST_NAME).isEqualToIgnoreCase("john").and().where(P_LAST_NAME)
					.isEqualToIgnoreCase("doe").toIterator().next();
			Vertex vJane = tx.find().vertices().where(P_FIRST_NAME).isEqualToIgnoreCase("jane").and().where(P_LAST_NAME)
					.isEqualToIgnoreCase("smith").toIterator().next();
			Vertex vJack = tx.find().vertices().where(P_FIRST_NAME).isEqualToIgnoreCase("jack").and().where(P_LAST_NAME)
					.isEqualToIgnoreCase("doe").toIterator().next();
			assertNotNull(vJohn);
			assertNotNull(vJane);
			assertNotNull(vJack);

			// add the relationship between John and Jane
			vJohn.addEdge("family", vJane, E_KIND, "married");
			vJane.addEdge("family", vJohn, E_KIND, "married");
			// add the relationship between Jack and Jane
			vJack.addEdge("family", vJane, E_KIND, "sister-in-law");
			vJane.addEdge("family", vJack, E_KIND, "brother-in-law");

			// change the nicknames of jane
			vJane.property(P_NICKNAMES, Sets.newHashSet("JD", "Jenny"));

			// add Sarah
			Vertex vSarah = tx.addVertex(T.id, "v4", P_FIRST_NAME, "Sarah", P_LAST_NAME, "Doe", P_GENDER, "female");
			vSarah.property(P_NICKNAMES, Sets.newHashSet("SD", "Sassie"));

			// link sarah's parents with her
			vJohn.addEdge("family", vSarah, E_KIND, "father");
			vJane.addEdge("family", vSarah, E_KIND, "mother");
			vJack.addEdge("family", vSarah, E_KIND, "uncle");

			assertCommitAssertCloseTx(tx,
					// assert function
					(final ChronoGraph g) -> {
						// make sure that sarah is detectable via query on her first name
						assertEquals(1, g.find().vertices().where(P_FIRST_NAME).isEqualToIgnoreCase("sarah").count());
						// make sure we also find sarah by her nick name
						assertEquals(1, g.find().vertices().where(P_NICKNAMES).containsIgnoreCase("sassie").count());
						// make sure we no longer find jane by her old nickname "JS"
						assertEquals(0, g.find().vertices().where(P_NICKNAMES).isEqualToIgnoreCase("JS").count());
						// make sure that sarah has a mother, a father and an uncle
						assertEquals(vJane, g.traversal().V(vSarah).inE("family").has(E_KIND, "mother").outV().next());
						assertEquals(vJohn, g.traversal().V(vSarah).inE("family").has(E_KIND, "father").outV().next());
						assertEquals(vJack, g.traversal().V(vSarah).inE("family").has(E_KIND, "uncle").outV().next());
					} ,
					// commit function
					(final ChronoGraph g) -> {
						// this lambda expression describes how to commit the first tx and open the next
						g.tx().commit();
						return graph.tx().createThreadedTx("Scenario A");
					}
			// end of statement
			);

			// make sure that none of the changes we just performed are visible on the master branch
			tx = graph.tx().createThreadedTx(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);

			// we should still find Jane Smith
			assertEquals(vJane, tx.find().vertices().where(P_FIRST_NAME).isEqualToIgnoreCase("jane").and()
					.where(P_LAST_NAME).isEqualToIgnoreCase("smith").toIterator().next());
			// there should not be a Jane Doe
			assertEquals(0, tx.find().vertices().where(P_FIRST_NAME).isEqualToIgnoreCase("jane").and()
					.where(P_LAST_NAME).isEqualToIgnoreCase("doe").count());
			// in particular, we shouldn't know sarah
			assertEquals(0, tx.find().vertices().where(P_FIRST_NAME).isEqualToIgnoreCase("sarah").count());

			// the branch should have a higher "now" timestamp than master
			assertTrue(graph.getNow(SCENARIO_A) > graph.getNow());

		} catch (Throwable t) {
			throw new AssertionError("Execution of Phase 3 failed! See root cause for details.", t);
		}
	}

	/**
	 * Performs phase 4 of the test.
	 *
	 * <p>
	 * This method will:
	 * <ul>
	 * <li>Create a new branch named "Scenario A.1", based on "Scenario A"
	 * <li>Remove a vertex from the graph during an incremental commit
	 * </ul>
	 *
	 * <p>
	 * <b>INFORMAL SCENARIO DESCRIPTION:</b><br>
	 * John dies in an accident. This is modeled as a vertex removal.
	 *
	 * <p>
	 * <b>RESULTING GRAPH:</b>
	 * <ul>
	 * <li><b>v1:</b> First name: "Jack", Last name: "Doe", Nick names: ["JD", "Jacky"]
	 * <li><b>v2:</b> First name: "Jane", Last name: "Doe", Nick names: ["JD", "Jenny"]
	 * <li><b>v3:</b> First name: "Jill", Last name: "Johnson", Nick names: ["JJ", "JiJo"]
	 * <li><b>v4:</b> First name: "Sarah", Last name: "Doe", Nick names: ["SD", "Sassie"]
	 * </ul>
	 *
	 * <ul>
	 * <li>Jane -[knows; kind = friend]->Jill
	 * <li>Jill -[knows; kind = friend]->Jane
	 * <li>Jack -[family; kind = sister-in-law]-> Jane
	 * <li>Jane -[family; kind = brother-in-law]-> Jack
	 * <li>Jane -[family; kind = mother]-> Sarah
	 * <li>Jack -[family; kind = uncle]-> Sarah
	 * </ul>
	 *
	 * @param graph
	 *            The graph to execute the procedure on. Must not be <code>null</code>.
	 */
	public static void runPhase4(final ChronoGraph graph) {
		try {
			// create the branch for the scenario
			GraphBranch master = graph.getBranchManager().getMasterBranch();
			assertNotNull(master);
			GraphBranch branchA = graph.getBranchManager().getBranch(SCENARIO_A);
			assertNotNull(branchA);
			GraphBranch branchA1 = graph.getBranchManager().createBranch(SCENARIO_A, SCENARIO_A1);
			assertNotNull(branchA1);
			assertEquals(branchA, branchA1.getOrigin());
			assertEquals(Lists.newArrayList(master, branchA), branchA1.getOriginsRecursive());

			// open a transaction
			ChronoGraph txGraph = graph.tx().createThreadedTx(SCENARIO_A1);

			// start an incremental commit process
			txGraph.tx().commitIncremental();

			// find John Doe
			Set<Vertex> johns = txGraph.traversal().V().has(P_FIRST_NAME, "John").has(P_LAST_NAME, "Doe").toSet();
			Vertex vJohn = Iterables.getOnlyElement(johns);
			assertNotNull(vJohn);

			// remove John Doe from the graph
			vJohn.remove();

			assertCommitAssertCloseTx(txGraph,
					// assert function
					(final ChronoGraph g) -> {
						// make sure that we can't find John anymore
						assertEquals(0, g.find().vertices().where(P_FIRST_NAME).isEqualToIgnoreCase("john").count());
						// find Jack, Jane and Sarah
						Vertex vJack = g.find().vertices().where(P_FIRST_NAME).isEqualTo("Jack").toIterator().next();
						Vertex vJane = g.find().vertices().where(P_FIRST_NAME).isEqualTo("Jane").toIterator().next();
						Vertex vSarah = g.find().vertices().where(P_FIRST_NAME).isEqualTo("Sarah").toIterator().next();
						// make sure that Jack has no brother anymore
						assertEquals(0,
								g.traversal().V(vJack).outE("family").has(E_KIND, "brother").inV().toSet().size());
						assertEquals(0,
								g.traversal().V(vJack).inE("family").has(E_KIND, "brother").outV().toSet().size());
						// make sure that Jane has no husband anymore
						assertEquals(0,
								g.traversal().V(vJane).outE("family").has(E_KIND, "married").inV().toSet().size());
						assertEquals(0,
								g.traversal().V(vJane).inE("family").has(E_KIND, "married").outV().toSet().size());
						// make sure that sarah has no father anymore
						assertEquals(0,
								g.traversal().V(vSarah).inE("family").has(E_KIND, "father").outV().toSet().size());
						// assert that we can't find John Doe by his nickname 'JD' anymore
						Set<Vertex> JDs = g.find().vertices().where(P_NICKNAMES).isEqualTo("JD").toSet();
						// there should be two 'JD's left: Jane Doe and Jack Doe
						assertEquals(2, JDs.size());
						assertTrue(JDs.contains(vJane));
						assertTrue(JDs.contains(vJack));
						// ... but John should be missing
						assertFalse(JDs.contains(vJohn));
					} ,
					// commit function
					(final ChronoGraph g) -> {
						g.tx().commit();
						return graph.tx().createThreadedTx(SCENARIO_A1);
					}
			// end of statement
			);

			// make sure that none of our changes are visible in the "Scenario A" base branch
			txGraph = graph.tx().createThreadedTx(SCENARIO_A);
			// make sure that we can still find John
			Vertex vJohn2 = txGraph.traversal().V().has(P_FIRST_NAME, "John").has(P_LAST_NAME, "Doe").next();
			assertNotNull(vJohn2);
			// make sure that we still find John by his nickname
			assertTrue(txGraph.find().vertices().where(P_NICKNAMES).isEqualTo("JD").toSet().contains(vJohn2));
			// john should still have a family
			Set<Edge> johnsFamilyEdges = txGraph.traversal().V(vJohn2).outE("family").toSet();
			// ... he should be the father of a child...
			assertEquals(1, johnsFamilyEdges.stream().filter(e -> e.value(E_KIND).equals("father")).count());
			// ... there should be one bother...
			assertEquals(1, johnsFamilyEdges.stream().filter(e -> e.value(E_KIND).equals("brother")).count());
			// ... and he should be married
			assertEquals(1, johnsFamilyEdges.stream().filter(e -> e.value(E_KIND).equals("married")).count());
			txGraph.tx().close();
		} catch (Throwable t) {
			throw new AssertionError("Execution of Phase 4 failed! See root cause for details.", t);
		}
	}

	// =====================================================================================================================
	// UTILITIES
	// =====================================================================================================================

	private static void assertFirstNameCountEquals(final ChronoGraph graph, final String firstName, final int count) {
		assertEquals(count, graph.find().vertices().where(P_FIRST_NAME).isEqualTo(firstName).count());
		assertEquals(count, graph.traversal().V().has(P_FIRST_NAME, firstName).toSet().size());
	}

	private static void assertLastNameCountEquals(final ChronoGraph graph, final String lastName, final int count) {
		assertEquals(count, graph.find().vertices().where(P_LAST_NAME).isEqualTo(lastName).count());
		assertEquals(count, graph.traversal().V().has(P_LAST_NAME, lastName).toSet().size());
	}

	@SuppressWarnings("unused")
	private static ChronoGraph assertCommitAssert(final ChronoGraph graph, final Consumer<ChronoGraph> assertion,
			final Function<ChronoGraph, ChronoGraph> commitFunction) {
		assertion.accept(graph);
		ChronoGraph newGraph = commitFunction.apply(graph);
		assertion.accept(newGraph);
		return newGraph;
	}

	private static void assertCommitAssertCloseTx(final ChronoGraph graph, final Consumer<ChronoGraph> assertion,
			final Function<ChronoGraph, ChronoGraph> commitFunction) {
		assertion.accept(graph);
		ChronoGraph newGraph = commitFunction.apply(graph);
		assertion.accept(newGraph);
		newGraph.tx().close();
	}
}
