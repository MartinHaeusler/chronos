package org.chronos.chronosphere.api.query;

public interface NumericCastableQueryStepBuilder<S, E> extends QueryStepBuilder<S, E> {

	/**
	 * Converts the elements in the current result set into {@link Byte}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Byte. If it is a Byte, it is cast down and
	 * forwarded. Otherwise, it is discarded, i.e. all non-Bytes will be filtered out. All <code>null</code> values will
	 * also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public NumericQueryStepBuilder<S, Byte> asByte();

	/**
	 * Converts the elements in the current result set into {@link Short}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Short. If it is a Short, it is cast down
	 * and forwarded. Otherwise, it is discarded, i.e. all non-Shorts will be filtered out. All <code>null</code> values
	 * will also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public NumericQueryStepBuilder<S, Short> asShort();

	/**
	 * Converts the elements in the current result set into {@link Integer}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Integer. If it is a Integer, it is cast
	 * down and forwarded. Otherwise, it is discarded, i.e. all non-Integers will be filtered out. All <code>null</code>
	 * values will also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public NumericQueryStepBuilder<S, Integer> asInteger();

	/**
	 * Converts the elements in the current result set into {@link Long}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Long. If it is a Long, it is cast down and
	 * forwarded. Otherwise, it is discarded, i.e. all non-Longs will be filtered out. All <code>null</code> values will
	 * also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public NumericQueryStepBuilder<S, Long> asLong();

	/**
	 * Converts the elements in the current result set into {@link Float}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Float. If it is a Float, it is cast down
	 * and forwarded. Otherwise, it is discarded, i.e. all non-Floats will be filtered out. All <code>null</code> values
	 * will also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public NumericQueryStepBuilder<S, Float> asFloat();

	/**
	 * Converts the elements in the current result set into {@link Double}s.
	 *
	 * <p>
	 * This method first checks if the element in question is an instance of Double. If it is a Double, it is cast down
	 * and forwarded. Otherwise, it is discarded, i.e. all non-Doubles will be filtered out. All <code>null</code>
	 * values will also be filtered out.
	 *
	 * @return The step builder, for method chaining. Never <code>null</code>.
	 */
	public NumericQueryStepBuilder<S, Double> asDouble();

}
