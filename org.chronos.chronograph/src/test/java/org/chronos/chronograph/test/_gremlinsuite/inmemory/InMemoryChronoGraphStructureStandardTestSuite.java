package org.chronos.chronograph.test._gremlinsuite.inmemory;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.junit.runner.RunWith;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = InMemoryChronoGraphProvider.class, graph = ChronoGraph.class)
public class InMemoryChronoGraphStructureStandardTestSuite {

}
