package org.chronos.chronodb.test.conflict;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.chronos.chronodb.api.conflict.AtomicConflict;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;
import org.chronos.chronodb.internal.impl.ConflictResolutionStrategyLoader;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class ConflictResolutionStrategyLoaderTest extends ChronoDBUnitTest {

	@Test
	public void canResolveDefaultStrategy() {
		// null produces the default strategy
		assertThat(this.load(null), is(ConflictResolutionStrategy.DO_NOT_MERGE));
		// empty string produces the default strategy
		assertThat(this.load(""), is(ConflictResolutionStrategy.DO_NOT_MERGE));
		// whitespace string produces the default strategy
		assertThat(this.load("   \t   "), is(ConflictResolutionStrategy.DO_NOT_MERGE));
	}

	@Test
	public void canResolvePredefinedStrategies() {
		assertThat(this.load("DO_NOT_MERGE"), is(ConflictResolutionStrategy.DO_NOT_MERGE));
		assertThat(this.load("OVERWRITE_WITH_SOURCE"), is(ConflictResolutionStrategy.OVERWRITE_WITH_SOURCE));
		assertThat(this.load("OVERWRITE_WITH_TARGET"), is(ConflictResolutionStrategy.OVERWRITE_WITH_TARGET));
	}

	@Test
	public void canLoadCustomClass() {
		String className = TestConflictResolutionStrategy.class.getName();
		assertThat(this.load(className), instanceOf(TestConflictResolutionStrategy.class));
	}

	@Test
	public void loadingFailsOnUnknownStrings() {
		try {
			this.load("ThisAintNoStrategy");
			fail("Managed to load non-existing conflict resolution strategy!");
		} catch (IllegalArgumentException expected) {
			// pass
		}
	}

	@Test
	public void loadingFailsIfClassDoesNotImplementInterface() {
		try {
			this.load(NotAConflictResolutionStrategy.class.getName());
			fail("Managed to use a class that does not implement " + ConflictResolutionStrategy.class.getName()
					+ "' as a conflict resolution strategy!");
		} catch (IllegalArgumentException expected) {
			// pass
		}
	}

	// =================================================================================================================
	// HELPER METHODS
	// =================================================================================================================

	/**
	 * Just a small syntactic helper that calls {@link ConflictResolutionStrategyLoader#load(String)}.
	 *
	 * @param name
	 *            the name of the strategy to load. May be <code>null</code>.
	 * @return the loaded strategy instance.
	 */
	private ConflictResolutionStrategy load(final String name) {
		return ConflictResolutionStrategyLoader.load(name);
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	public static class TestConflictResolutionStrategy implements ConflictResolutionStrategy {

		@Override
		public Object resolve(final AtomicConflict conflict) {
			// just for testing...
			return null;
		}

	}

	public static class NotAConflictResolutionStrategy {

	}
}
