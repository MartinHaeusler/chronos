package org.chronos.chronograph.internal.impl.transaction.threaded;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronograph.api.builder.index.IndexBuilderStarter;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;

public class ThreadedChronoGraphIndexManager implements ChronoGraphIndexManager, ChronoGraphIndexManagerInternal {

	private final ChronoThreadedTransactionGraph graph;
	private final ChronoGraphIndexManagerInternal wrappedManager;

	public ThreadedChronoGraphIndexManager(final ChronoThreadedTransactionGraph graph, final String branchName) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.graph = graph;
		this.wrappedManager = (ChronoGraphIndexManagerInternal) this.graph.getOriginalGraph()
				.getIndexManager(branchName);
	}

	@Override
	public void addIndex(final ChronoGraphIndex index) {
		this.wrappedManager.executeOnGraph(this.graph, () -> {
			this.wrappedManager.addIndex(index);
		});
	}

	@Override
	public Iterator<String> findVertexIdsByIndexedProperties(final Set<SearchSpecification<?>> searchSpecifications) {
		return this.wrappedManager.executeOnGraph(this.graph, () -> {
			return this.wrappedManager.findVertexIdsByIndexedProperties(searchSpecifications);
		});
	}

	@Override
	public Iterator<String> findEdgeIdsByIndexedProperties(final Set<SearchSpecification<?>> searchSpecifications) {
		return this.wrappedManager.executeOnGraph(this.graph, () -> {
			return this.wrappedManager.findEdgeIdsByIndexedProperties(searchSpecifications);
		});
	}

	@Override
	public IndexBuilderStarter create() {
		return this.wrappedManager.executeOnGraph(this.graph, () -> {
			return this.wrappedManager.create();
		});
	}

	@Override
	public Set<ChronoGraphIndex> getIndexedPropertiesOf(final Class<? extends Element> clazz) {
		return this.wrappedManager.executeOnGraph(this.graph, () -> {
			return this.wrappedManager.getIndexedPropertiesOf(clazz);
		});
	}

	@Override
	public void reindexAll() {
		this.wrappedManager.executeOnGraph(this.graph, () -> {
			this.wrappedManager.reindexAll();
		});
	}

	@Override
	public void reindex(final ChronoGraphIndex index) {
		this.wrappedManager.executeOnGraph(this.graph, () -> {
			this.wrappedManager.reindexAll();
		});
	}

	@Override
	public void dropIndex(final ChronoGraphIndex index) {
		this.wrappedManager.executeOnGraph(this.graph, () -> {
			this.wrappedManager.dropIndex(index);
		});
	}

	@Override
	public void dropAllIndices() {
		this.wrappedManager.executeOnGraph(this.graph, () -> {
			this.wrappedManager.dropAllIndices();
		});
	}

	@Override
	public boolean isReindexingRequired() {
		return this.wrappedManager.executeOnGraph(this.graph, () -> {
			return this.wrappedManager.isReindexingRequired();
		});
	}

	@Override
	public Set<ChronoGraphIndex> getDirtyIndices() {
		return this.wrappedManager.executeOnGraph(this.graph, () -> {
			return this.wrappedManager.getDirtyIndices();
		});
	}

	@Override
	public <T> T executeOnGraph(final ChronoGraphInternal graph, final Callable<T> job) {
		throw new UnsupportedOperationException(
				"executeOnGraph(...) is not supported in a threaded transaction graph!");
	}

	@Override
	public void executeOnGraph(final ChronoGraphInternal graph, final Runnable job) {
		throw new UnsupportedOperationException(
				"executeOnGraph(...) is not supported in a threaded transaction graph!");
	}

}
