package org.chronos.chronodb.internal.impl.temporal;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.Period;

public class GetResultImpl<T> implements GetResult<T> {

	// =====================================================================================================================
	// STATIC FACTORY METHODS
	// =====================================================================================================================

	public static <T> GetResult<T> createNoValueResult(final QualifiedKey requestedKey, final Period range) {
		checkNotNull(requestedKey, "Precondition violation - argument 'requestedKey' must not be NULL!");
		checkNotNull(range, "Precondition violation - argument 'range' must not be NULL!");
		return new GetResultImpl<T>(requestedKey, null, range, false);
	}

	public static <T> GetResult<T> create(final QualifiedKey requestedKey, final T value, final Period range) {
		checkNotNull(requestedKey, "Precondition violation - argument 'requestedKey' must not be NULL!");
		checkNotNull(range, "Precondition violation - argument 'range' must not be NULL!");
		return new GetResultImpl<T>(requestedKey, value, range, true);
	}

	public static <T> GetResult<T> alterPeriod(final GetResult<T> getResult, final Period newPeriod) {
		checkNotNull(getResult, "Precondition violation - argument 'getResult' must not be NULL!");
		checkNotNull(newPeriod, "Precondition violation - argument 'newPeriod' must not be NULL!");
		return new GetResultImpl<T>(getResult.getRequestedKey(), getResult.getValue(), newPeriod, getResult.isHit());
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final QualifiedKey requestedKey;
	private final T value;
	private final Period range;
	private final boolean isHit;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	private GetResultImpl(final QualifiedKey requestedKey, final T value, final Period range, final boolean isHit) {
		checkNotNull(requestedKey, "Precondition violation - argument 'requestedKey' must not be NULL!");
		checkNotNull(range, "Precondition violation - argument 'range' must not be NULL!");
		this.requestedKey = requestedKey;
		this.value = value;
		this.range = range;
		this.isHit = isHit;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public QualifiedKey getRequestedKey() {
		return this.requestedKey;
	}

	@Override
	public Period getPeriod() {
		return this.range;
	}

	@Override
	public T getValue() {
		return this.value;
	}

	@Override
	public boolean isHit() {
		return this.isHit;
	}

	@Override
	public String toString() {
		return "GetResult[key=" + this.requestedKey + ", period=" + this.getPeriod() + ", value=" + this.getValue()
				+ ", isHit=" + this.isHit() + "]";
	}

}
