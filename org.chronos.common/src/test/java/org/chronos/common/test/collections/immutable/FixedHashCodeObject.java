package org.chronos.common.test.collections.immutable;

import static com.google.common.base.Preconditions.*;

import org.chronos.common.collections.util.BitFieldUtil;

public class FixedHashCodeObject {

	// =====================================================================================================================
	// FACTORY METHODS
	// =====================================================================================================================

	public static FixedHashCodeObject create(final int hashCode) {
		return new FixedHashCodeObject(new Object(), hashCode);
	}

	public static FixedHashCodeObject create(final String hashCodeBinary) {
		return new FixedHashCodeObject(new Object(), BitFieldUtil.fromBinary(hashCodeBinary));
	}

	public static FixedHashCodeObject create(final Object identity, final int hashCode) {
		checkNotNull(identity, "Precondition violation - argument 'identity' must not be NULL!");
		return new FixedHashCodeObject(identity, hashCode);
	}

	public static FixedHashCodeObject create(final Object identity, final String hashCodeBinary) {
		checkNotNull(identity, "Precondition violation - argument 'identity' must not be NULL!");
		checkNotNull(hashCodeBinary, "Precondition violation - argument 'hashCodeBinary' must not be NULL!");
		return new FixedHashCodeObject(identity, BitFieldUtil.fromBinary(hashCodeBinary));
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final Object identity;
	private final int hashCode;

	private FixedHashCodeObject(final Object identity, final int hashCode) {
		checkNotNull(identity, "Precondition violation - argument 'identity' must not be NULL!");
		this.identity = identity;
		this.hashCode = hashCode;
	}

	@Override
	public int hashCode() {
		return this.hashCode;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj instanceof FixedHashCodeObject) {
			return this.identity == ((FixedHashCodeObject) obj).identity;
		}
		return this.identity == obj;
	}

	@Override
	public String toString() {
		return "FixedHash[id='" + this.identity + "', hash=" + this.hashCode + "]";
	}
}
