package org.chronos.chronosphere.test.emf.estore.base;

public enum EmfAPI {

	REFERENCE_IMPLEMENTATION, CHRONOS_TRANSIENT, CHRONOS_GRAPH;

	public boolean requiresChronoSphere() {
		if (CHRONOS_GRAPH.equals(this)) {
			return true;
		} else {
			return false;
		}
	}

}
