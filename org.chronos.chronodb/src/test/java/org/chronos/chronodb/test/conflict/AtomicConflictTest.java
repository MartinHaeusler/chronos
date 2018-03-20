package org.chronos.chronodb.test.conflict;

import static org.junit.Assert.*;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.conflict.AtomicConflict;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.impl.conflict.AncestorFetcher;
import org.chronos.chronodb.internal.impl.conflict.AtomicConflictImpl;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class AtomicConflictTest extends ChronoDBUnitTest {

	@Test
	public void ancestorIsOnlyFetchedOnce() {
		ChronoIdentifier sourceKey = ChronoIdentifier.create("master", 1000, "default", "hello");
		Object sourceValue = "baz";
		ChronoIdentifier targetKey = ChronoIdentifier.create("master", 900, "default", "hello");
		Object targetValue = "bar";

		AncestorFetcher fetcher = (ts, sk, tk) -> {
			ChronoIdentifier ancestorKey = ChronoIdentifier.create("master", 500, "default", "hello");
			Object ancestorValue = "world";
			return Pair.of(ancestorKey, ancestorValue);
		};

		AtomicConflict conflict = new AtomicConflictImpl(0, sourceKey, sourceValue, targetKey, targetValue, fetcher);

		// try to fetch the ancestor twice
		ChronoIdentifier ancestorKey = conflict.getCommonAncestorKey();
		Object ancestorValue = conflict.getCommonAncestorValue();
		ChronoIdentifier ancestorKey2 = conflict.getCommonAncestorKey();
		Object ancestorValue2 = conflict.getCommonAncestorValue();

		// we should get the very SAME objects, even though the ancestor fetcher
		// produces new instances on every call (because AtomicConflictImpl has a cache)
		assertTrue(ancestorKey == ancestorKey2);
		assertTrue(ancestorValue == ancestorValue2);
	}

	@Test
	public void canDealWithDeletions() {
		// this is a hypothetical case where all values represent deletions.
		// This would not be an "actual" conflict, but we use it to test NULL handling.

		ChronoIdentifier sourceKey = ChronoIdentifier.create("master", 1000, "default", "hello");
		Object sourceValue = null;
		ChronoIdentifier targetKey = ChronoIdentifier.create("master", 900, "default", "hello");
		Object targetValue = null;

		AncestorFetcher fetcher = (ts, sk, tk) -> {
			ChronoIdentifier ancestorKey = ChronoIdentifier.create("master", 500, "default", "hello");
			Object ancestorValue = null;
			return Pair.of(ancestorKey, ancestorValue);
		};

		AtomicConflict conflict = new AtomicConflictImpl(0, sourceKey, sourceValue, targetKey, targetValue, fetcher);

		assertNull(conflict.getSourceValue());
		assertNull(conflict.getTargetValue());
		assertNull(conflict.getCommonAncestorValue());
	}

}
