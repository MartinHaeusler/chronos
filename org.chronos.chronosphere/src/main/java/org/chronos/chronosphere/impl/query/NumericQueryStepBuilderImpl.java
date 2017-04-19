package org.chronos.chronosphere.impl.query;

import java.util.Iterator;
import java.util.Optional;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.api.query.NumericQueryStepBuilder;
import org.chronos.chronosphere.api.query.QueryStepBuilderInternal;

public class NumericQueryStepBuilderImpl<S, E extends Number> extends AbstractQueryStepBuilder<S, E>
		implements NumericQueryStepBuilder<S, E> {

	public NumericQueryStepBuilderImpl(final QueryStepBuilderInternal<S, ?> previous,
			final GraphTraversal<?, E> traversal) {
		super(previous, traversal);
	}

	@Override
	public Optional<Double> sumAsDouble() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		Iterator<E> iterator = this.getTraversal();
		Double sum = null;
		while (iterator.hasNext()) {
			E next = iterator.next();
			if (next == null) {
				continue;
			}
			if (sum != null) {
				sum += next.doubleValue();
			} else {
				sum = next.doubleValue();
			}
		}
		return Optional.ofNullable(sum);
	}

	@Override
	public Optional<Long> sumAsLong() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		Iterator<E> iterator = this.getTraversal();
		Long sum = null;
		while (iterator.hasNext()) {
			E next = iterator.next();
			if (next == null) {
				continue;
			}
			if (sum != null) {
				sum += next.longValue();
			} else {
				sum = next.longValue();
			}
		}
		return Optional.ofNullable(sum);
	}

	@Override
	public Optional<Double> average() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		Iterator<E> iterator = this.getTraversal();
		Double sum = null;
		long count = 0L;
		while (iterator.hasNext()) {
			count++;
			E next = iterator.next();
			if (next == null) {
				continue;
			}
			if (sum != null) {
				sum += next.doubleValue();
			} else {
				sum = next.doubleValue();
			}
		}
		if (count == 0) {
			return Optional.empty();
		}
		return Optional.of(sum / count);
	}

	@Override
	public Optional<Double> maxAsDouble() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		Iterator<E> iterator = this.getTraversal();
		Double max = null;
		while (iterator.hasNext()) {
			E element = iterator.next();
			if (element == null) {
				continue;
			}
			if (max == null || element.doubleValue() > max) {
				max = element.doubleValue();
			}
		}
		return Optional.ofNullable(max);
	}

	@Override
	public Optional<Long> maxAsLong() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		Iterator<E> iterator = this.getTraversal();
		Long max = null;
		while (iterator.hasNext()) {
			E element = iterator.next();
			if (element == null) {
				continue;
			}
			if (max == null || element.longValue() > max) {
				max = element.longValue();
			}
		}
		return Optional.ofNullable(max);
	}

	@Override
	public Optional<Double> minAsDouble() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		Iterator<E> iterator = this.getTraversal();
		Double min = null;
		while (iterator.hasNext()) {
			E element = iterator.next();
			if (element == null) {
				continue;
			}
			if (min == null || element.doubleValue() < min) {
				min = element.doubleValue();
			}
		}
		return Optional.ofNullable(min);
	}

	@Override
	public Optional<Long> minAsLong() {
		this.assertModificationsAllowed();
		this.preventAnyFurtherModifications();
		Iterator<E> iterator = this.getTraversal();
		Long min = null;
		while (iterator.hasNext()) {
			E element = iterator.next();
			if (element == null) {
				continue;
			}
			if (min == null || element.longValue() < min) {
				min = element.longValue();
			}
		}
		return Optional.ofNullable(min);
	}

	@Override
	public NumericQueryStepBuilder<S, Long> round() {
		this.assertModificationsAllowed();
		GraphTraversal<?, Long> newTraversal = this.getTraversal().map(traverser -> {
			if (traverser == null || traverser.get() == null) {
				// skip NULL values
				return null;
			}
			E element = traverser.get();
			if (element == null) {
				return null;
			}
			return Math.round(element.doubleValue());
		});
		return new NumericQueryStepBuilderImpl<>(this, newTraversal);
	}

	@Override
	public NumericQueryStepBuilder<S, Integer> roundToInt() {
		this.assertModificationsAllowed();
		GraphTraversal<?, Integer> newTraversal = this.getTraversal().map(traverser -> {
			if (traverser == null || traverser.get() == null) {
				// skip NULL values
				return null;
			}
			E element = traverser.get();
			if (element == null) {
				return null;
			}
			return (int) Math.round(element.doubleValue());
		});
		return new NumericQueryStepBuilderImpl<>(this, newTraversal);
	}

	@Override
	public NumericQueryStepBuilder<S, Long> floor() {
		this.assertModificationsAllowed();
		GraphTraversal<?, Long> newTraversal = this.getTraversal().map(traverser -> {
			if (traverser == null || traverser.get() == null) {
				// skip NULL values
				return null;
			}
			E element = traverser.get();
			if (element instanceof Float || element instanceof Double) {
				return (long) Math.floor(element.doubleValue());
			} else {
				// in any other case, we already have a "whole" number
				return element.longValue();
			}
		});
		return new NumericQueryStepBuilderImpl<>(this, newTraversal);
	}

	@Override
	public NumericQueryStepBuilder<S, Long> ceil() {
		this.assertModificationsAllowed();
		GraphTraversal<?, Long> newTraversal = this.getTraversal().map(traverser -> {
			if (traverser == null || traverser.get() == null) {
				// skip NULL values
				return null;
			}
			E element = traverser.get();
			if (element instanceof Float || element instanceof Double) {
				return (long) Math.ceil(element.doubleValue());
			} else {
				// in any other case, we already have a "whole" number
				return element.longValue();
			}
		});
		return new NumericQueryStepBuilderImpl<>(this, newTraversal);
	}

	@Override
	public NumericQueryStepBuilder<S, Byte> asByte() {
		this.assertModificationsAllowed();
		GraphTraversal<?, Byte> newTraversal = this.castTraversalToNumeric(Number::byteValue);
		return new NumericQueryStepBuilderImpl<>(this, newTraversal);
	}

	@Override
	public NumericQueryStepBuilder<S, Short> asShort() {
		this.assertModificationsAllowed();
		GraphTraversal<?, Short> newTraversal = this.castTraversalToNumeric(Number::shortValue);
		return new NumericQueryStepBuilderImpl<>(this, newTraversal);
	}

	@Override
	public NumericQueryStepBuilder<S, Integer> asInteger() {
		this.assertModificationsAllowed();
		GraphTraversal<?, Integer> newTraversal = this.castTraversalToNumeric(Number::intValue);
		return new NumericQueryStepBuilderImpl<>(this, newTraversal);
	}

	@Override
	public NumericQueryStepBuilder<S, Long> asLong() {
		this.assertModificationsAllowed();
		GraphTraversal<?, Long> newTraversal = this.castTraversalToNumeric(Number::longValue);
		return new NumericQueryStepBuilderImpl<>(this, newTraversal);
	}

	@Override
	public NumericQueryStepBuilder<S, Float> asFloat() {
		this.assertModificationsAllowed();
		GraphTraversal<?, Float> newTraversal = this.castTraversalToNumeric(Number::floatValue);
		return new NumericQueryStepBuilderImpl<>(this, newTraversal);
	}

	@Override
	public NumericQueryStepBuilder<S, Double> asDouble() {
		this.assertModificationsAllowed();
		GraphTraversal<?, Double> newTraversal = this.castTraversalToNumeric(Number::doubleValue);
		return new NumericQueryStepBuilderImpl<>(this, newTraversal);
	}

}
