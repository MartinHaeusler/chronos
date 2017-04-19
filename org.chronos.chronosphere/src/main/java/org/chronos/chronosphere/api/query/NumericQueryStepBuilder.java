package org.chronos.chronosphere.api.query;

import java.util.Optional;

public interface NumericQueryStepBuilder<S, E extends Number>
		extends QueryStepBuilder<S, E>, NumericCastableQueryStepBuilder<S, E> {

	public Optional<Double> sumAsDouble();

	public Optional<Long> sumAsLong();

	public Optional<Double> average();

	public Optional<Double> maxAsDouble();

	public Optional<Long> maxAsLong();

	public Optional<Double> minAsDouble();

	public Optional<Long> minAsLong();

	public NumericQueryStepBuilder<S, Long> round();

	public NumericQueryStepBuilder<S, Integer> roundToInt();

	public NumericQueryStepBuilder<S, Long> floor();

	public NumericQueryStepBuilder<S, Long> ceil();

}