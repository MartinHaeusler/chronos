package org.chronos.benchmarks.util.statistics;

public interface ProbabilityDistribution<E> {

	public static <E> DiscreteProbabilityDistributionBuilder<E> discrete() {
		return DiscreteProbabilityDistribution.<E> builder();
	}

	public E nextEvent();

}
