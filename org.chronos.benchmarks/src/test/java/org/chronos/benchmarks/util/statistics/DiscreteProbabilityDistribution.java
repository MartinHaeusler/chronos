package org.chronos.benchmarks.util.statistics;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;

import com.google.common.collect.Maps;

public class DiscreteProbabilityDistribution<E> implements ProbabilityDistribution<E> {

	// =====================================================================================================================
	// BUILDER
	// =====================================================================================================================

	public static <E> DiscreteProbabilityDistributionBuilder<E> builder() {
		return new DiscreteProbabilityDistributionBuilder<E>();
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final Map<E, Double> normalizedDistribution;
	private final NavigableMap<Double, E> weightRangeToEvent = Maps.newTreeMap();
	private final Random random;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	/* default */ DiscreteProbabilityDistribution(final Map<E, Double> normalizedDistribution, final Random random) {
		checkNotNull(normalizedDistribution,
				"Precondition violation - argument 'normalizedDistribution' must not be NULL!");
		checkArgument(normalizedDistribution.isEmpty() == false,
				"Precondition violation - the event distribution must contain at least one possible event (weight > 0)!");
		checkNotNull(random, "Precondition violation - argument 'random' must not be NULL!");
		this.normalizedDistribution = Maps.newHashMap(normalizedDistribution);
		double total = 0;
		for (Entry<E, Double> entry : normalizedDistribution.entrySet()) {
			total += entry.getValue();
			this.weightRangeToEvent.put(total, entry.getKey());
		}
		this.random = random;
	}

	@Override
	public E nextEvent() {
		// note: random.nextDouble() delivers a value between 0 and 1, our weights have been normalized such that
		// the sum of all weights is 1, so we don't need to do any more modificiations to the random value.
		double value = this.random.nextDouble();
		return this.weightRangeToEvent.ceilingEntry(value).getValue();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("RandomDistribution[");
		String separator = "";
		for (Entry<E, Double> entry : this.normalizedDistribution.entrySet()) {
			builder.append(separator);
			separator = ", ";
			builder.append("'");
			builder.append(entry.getKey());
			builder.append("': ");
			builder.append(String.format("%1.2f%%", entry.getValue() * 100));
		}
		builder.append("]");
		return builder.toString();
	}

}
