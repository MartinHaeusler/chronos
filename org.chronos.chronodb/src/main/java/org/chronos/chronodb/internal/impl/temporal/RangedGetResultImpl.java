package org.chronos.chronodb.internal.impl.temporal;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.RangedGetResult;

public class RangedGetResultImpl<T> implements RangedGetResult<T> {

	// =====================================================================================================================
	// STATIC FACTORY METHODS
	// =====================================================================================================================

	public static <T> RangedGetResult<T> createNoValueResult(final QualifiedKey requestedKey, final Period range) {
		checkNotNull(requestedKey, "Precondition violation - argument 'requestedKey' must not be NULL!");
		checkNotNull(range, "Precondition violation - argument 'range' must not be NULL!");
		return new RangedGetResultImpl<T>(requestedKey, null, range, false);
	}

	public static <T> RangedGetResult<T> create(final QualifiedKey requestedKey, final T value, final Period range) {
		checkNotNull(requestedKey, "Precondition violation - argument 'requestedKey' must not be NULL!");
		checkNotNull(range, "Precondition violation - argument 'range' must not be NULL!");
		return new RangedGetResultImpl<T>(requestedKey, value, range, true);
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

	private RangedGetResultImpl(final QualifiedKey requestedKey, final T value, final Period range,
			final boolean isHit) {
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
	public Period getRange() {
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
		return "RangedGetResult{range=" + this.getRange() + ", value=" + this.getValue() + ", isHit=" + this.isHit()
				+ "}";
	}
}
