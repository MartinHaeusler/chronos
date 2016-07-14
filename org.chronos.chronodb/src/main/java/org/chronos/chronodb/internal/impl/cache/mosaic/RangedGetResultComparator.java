package org.chronos.chronodb.internal.impl.cache.mosaic;

import java.util.Comparator;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.RangedGetResult;

public class RangedGetResultComparator implements Comparator<RangedGetResult<?>> {

	// =====================================================================================================================
	// SINGLETON PATTERN
	// =====================================================================================================================

	private static RangedGetResultComparator INSTANCE;

	static {
		INSTANCE = new RangedGetResultComparator();
	}

	public static RangedGetResultComparator getInstance() {
		return INSTANCE;
	}

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	private RangedGetResultComparator() {
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public int compare(final RangedGetResult<?> o1, final RangedGetResult<?> o2) {
		if (o1 == null && o2 == null) {
			return 0;
		} else if (o1 != null && o2 == null) {
			return 1;
		} else if (o1 == null && o2 != null) {
			return -1;
		} else {
			// for a total ordering, we need to make sure that the requested keys were the same.
			// If the requested keys were different, we compare them instead.
			if (o1.getRequestedKey().equals(o2.getRequestedKey()) == false) {
				QualifiedKey qKey1 = o1.getRequestedKey();
				QualifiedKey qKey2 = o2.getRequestedKey();
				int keyspaceCompare = qKey1.getKeyspace().compareTo(qKey2.getKeyspace());
				if (keyspaceCompare != 0) {
					return keyspaceCompare;
				} else {
					return qKey1.getKey().compareTo(qKey2.getKey());
				}
			}
			// we want descending order.
			// Ranges are ordered by their lower bound, which are of type Long.
			// Long.compareTo(...) creates an ascending order, so we invert it
			// by multiplying the result by -1.
			return o1.getRange().compareTo(o2.getRange()) * -1;
		}
	}

}
