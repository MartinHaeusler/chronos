package org.chronos.chronograph.test.gremlin;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.T;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@Category(PerformanceTest.class)
public class GremlinPerformanceTest extends AllChronoGraphBackendsTest {

	@Test
	public void canExecuteParallelVertexCounts() {
		ChronoGraph graph = this.getGraph();
		int vertexCount = 100_000;
		{ // insert test data
			graph.tx().open();
			for (int i = 0; i < vertexCount; i++) {
				graph.addVertex(T.id, String.valueOf(i));
			}
			graph.tx().commit();
		}
		int threadCount = 15;
		Set<Thread> threads = Sets.newHashSet();
		List<Throwable> exceptions = Collections.synchronizedList(Lists.newArrayList());
		List<Thread> successfullyTerminatedThreads = Collections.synchronizedList(Lists.newArrayList());
		for (int i = 0; i < threadCount; i++) {
			Thread thread = new Thread(() -> {
				try {
					graph.tx().open();
					long count = graph.traversal().V().count().next();
					assertEquals(vertexCount, count);
					graph.tx().close();
					successfullyTerminatedThreads.add(Thread.currentThread());
				} catch (Throwable t) {
					System.err.println("Error in Thread [" + Thread.currentThread().getName() + "]");
					t.printStackTrace();
					exceptions.add(t);
				}
			});
			thread.setName("ReadWorker" + i);
			threads.add(thread);
			thread.start();
		}
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				throw new RuntimeException("Waiting for thread was interrupted.", e);
			}
		}
		assertEquals(0, exceptions.size());
		assertEquals(threadCount, successfullyTerminatedThreads.size());
	}

}
