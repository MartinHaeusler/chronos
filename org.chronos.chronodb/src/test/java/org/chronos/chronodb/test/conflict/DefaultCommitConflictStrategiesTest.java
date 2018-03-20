package org.chronos.chronodb.test.conflict;

import static org.junit.Assert.*;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.conflict.AtomicConflict;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitConflictException;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.impl.conflict.AncestorFetcher;
import org.chronos.chronodb.internal.impl.conflict.AtomicConflictImpl;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class DefaultCommitConflictStrategiesTest extends ChronoDBUnitTest {

	@Test
	public void overwriteWithSourceWorks() {
		ConflictResolutionStrategy strategy = ConflictResolutionStrategy.OVERWRITE_WITH_SOURCE;
		AtomicConflict conflict = this.createConflict();
		Object resolution = strategy.resolve(conflict);
		assertEquals(conflict.getSourceValue(), resolution);
	}

	@Test
	public void overwriteWithTargetWorks() {
		ConflictResolutionStrategy strategy = ConflictResolutionStrategy.OVERWRITE_WITH_TARGET;
		AtomicConflict conflict = this.createConflict();
		Object resolution = strategy.resolve(conflict);
		assertEquals(conflict.getTargetValue(), resolution);
	}

	@Test
	public void doNotMergeWorks() {
		ConflictResolutionStrategy strategy = ConflictResolutionStrategy.DO_NOT_MERGE;
		AtomicConflict conflict = this.createConflict();
		try {
			strategy.resolve(conflict);
			fail("Managed to merge a conflict with default strategy: DO_NOT_MERGE");
		} catch (ChronoDBCommitConflictException expected) {
			// pass
		}
	}

	// =================================================================================================================
	// HELPER METHODS
	// =================================================================================================================

	private AtomicConflict createConflict() {
		ChronoIdentifier sourceKey = ChronoIdentifier.create("master", 1000, "default", "hello");
		Object sourceValue = "baz";
		ChronoIdentifier targetKey = ChronoIdentifier.create("master", 900, "default", "hello");
		Object targetValue = "bar";

		AncestorFetcher fetcher = (ts, sk, tk) -> {
			ChronoIdentifier ancestorKey = ChronoIdentifier.create("master", 500, "default", "hello");
			Object ancestorValue = "world";
			return Pair.of(ancestorKey, ancestorValue);
		};

		AtomicConflict conflict = new AtomicConflictImpl(0L, sourceKey, sourceValue, targetKey, targetValue, fetcher);
		return conflict;
	}
}
