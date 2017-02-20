package org.chronos.chronodb.internal.impl.cache.mosaic;

import java.util.Comparator;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.GetResult;

public class GetResultComparator implements Comparator<GetResult<?>> {

	// =====================================================================================================================
	// SINGLETON PATTERN
	// =====================================================================================================================

	private static GetResultComparator INSTANCE;

	static {
		INSTANCE = new GetResultComparator();
	}

	public static GetResultComparator getInstance() {
		return INSTANCE;
	}

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	private GetResultComparator() {
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public int compare(final GetResult<?> o1, final GetResult<?> o2) {
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
			int lowerBoundCompare = Long.compare(o1.getPeriod().getLowerBound(), o2.getPeriod().getLowerBound()) * -1;
			// if the lower bound comparison is non-zero, we have our ordering.
			if (lowerBoundCompare != 0) {
				return lowerBoundCompare;
			}
			// the lower-bound compare is zero, so we compare the upper bounds.
			return Long.compare(o1.getPeriod().getUpperBound(), o2.getPeriod().getUpperBound()) * -1;
		}
	}

}
