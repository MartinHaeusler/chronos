package org.chronos.chronograph.test._gremlinsuite.inmemory;

import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.impl.structure.graph.AbstractChronoElement;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoGraphVariables;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoProperty;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexProperty;
import org.chronos.chronograph.internal.impl.structure.graph.StandardChronoGraph;
import org.chronos.chronograph.internal.impl.structure.record.EdgeRecord;
import org.chronos.chronograph.internal.impl.structure.record.EdgeTargetRecord;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record.VertexPropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record.VertexRecord;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class InMemoryChronoGraphProvider extends AbstractGraphProvider implements GraphProvider {

	@Override
	public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName, final GraphData loadGraphWith) {
		Map<String, Object> baseConfig = Maps.newHashMap();
		baseConfig.put(Graph.GRAPH, ChronoGraph.class.getName());
		baseConfig.put(ChronoDBConfiguration.STORAGE_BACKEND, ChronosBackend.INMEMORY.toString());
		return baseConfig;
	}

	@Override
	public void clear(final Graph graph, final Configuration configuration) throws Exception {
		if (graph == null) {
			return;
		}
		ChronoGraph chronoGraph = (ChronoGraph) graph;
		chronoGraph.close();
	}

	@Override
	@SuppressWarnings("rawtypes")
	public Set<Class> getImplementations() {
		Set<Class> implementations = Sets.newHashSet();
		implementations.add(StandardChronoGraph.class);
		implementations.add(ChronoVertexImpl.class);
		implementations.add(ChronoEdgeImpl.class);
		implementations.add(AbstractChronoElement.class);
		implementations.add(ChronoProperty.class);
		implementations.add(ChronoVertexProperty.class);
		implementations.add(ChronoGraphVariables.class);
		implementations.add(VertexRecord.class);
		implementations.add(EdgeRecord.class);
		implementations.add(VertexPropertyRecord.class);
		implementations.add(PropertyRecord.class);
		implementations.add(EdgeTargetRecord.class);
		return implementations;
	}

	@Override
	public Object convertId(final Object id, final Class<? extends Element> c) {
		return String.valueOf(id);
	}

}
