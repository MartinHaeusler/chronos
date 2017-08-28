package org.chronos.chronodb.internal.api.migration;

import java.util.List;

import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.migration.annotations.Migration;
import org.chronos.chronodb.internal.impl.migration.MigrationChainImpl;
import org.chronos.common.version.ChronosVersion;

public interface MigrationChain<DBTYPE extends ChronoDBInternal> extends Iterable<Class<? extends ChronosMigration<DBTYPE>>> {

	// =================================================================================================================
	// STATIC FACTORY METHODS
	// =================================================================================================================

	/**
	 * Creates a new {@link MigrationChain} from the given Java package name.
	 *
	 * <p>
	 * This method will scan all classes in the package, looking for classes that extend {@link ChronosMigration} and have an <code>@</code>{@link Migration} annotation.
	 *
	 * <p>
	 * The following conditions apply on the {@link ChronosMigration} classes:
	 * <ul>
	 * <li>The classes must be top-level classes, i.e. they must not be nested.
	 * <li>The classes must implement {@link ChronosMigration}.
	 * <li>The classes must be annotated with <code>@</code>{@link Migration} (directly; inheriting this annotation is not supported).
	 * <li>The classes must have a default constructor.
	 * <li>The version ranges given by the <code>@</code>{@link Migration} interfaces must not intersect.
	 * </ul>
	 *
	 * @param qualifiedPackageName
	 *            The fully qualified name of the java package to scan. Must not be <code>null</code>.
	 *
	 * @return The migration chain of classes found in the given package. Never <code>null</code>, may be empty.
	 */
	public static <DBTYPE extends ChronoDBInternal> MigrationChain<DBTYPE> fromPackage(final String qualifiedPackageName) {
		return MigrationChainImpl.fromPackage(qualifiedPackageName);
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	/**
	 * Returns the migration classes found in this chain.
	 *
	 * @return The list of migration classes (unmodifiable view). May be empty, but never <code>null</code>.
	 */
	public List<Class<? extends ChronosMigration<DBTYPE>>> getMigrationClasses();

	/**
	 * Produces a sub-migration-chain from this instance by limiting the migration classes to the ones starting at or after the given version (inclusive).
	 *
	 * @param from
	 *            The minimal chronos version to be migrated from in the result chain (inclusive). Must not be <code>null</code>.
	 * @return The new migration chain with the limited scope. Never <code>null</code>, may be empty.
	 */
	public MigrationChain<DBTYPE> startingAt(ChronosVersion from);

	/**
	 * Executes this migration chain on the given {@link ChronoDBInternal ChronoDB} instance.
	 *
	 * @param chronoDB
	 *            The DB to migrate. Must not be <code>null</code>.
	 */
	public void execute(DBTYPE chronoDB);

}
