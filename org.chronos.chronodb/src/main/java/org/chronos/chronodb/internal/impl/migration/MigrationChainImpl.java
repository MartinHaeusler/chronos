package org.chronos.chronodb.internal.impl.migration;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.migration.ChronosMigration;
import org.chronos.chronodb.internal.api.migration.MigrationChain;
import org.chronos.chronodb.internal.api.migration.annotations.Migration;
import org.chronos.common.exceptions.ChronosIOException;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.version.ChronosVersion;

import com.google.common.collect.Lists;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

public final class MigrationChainImpl<DBTYPE extends ChronoDBInternal> implements MigrationChain<DBTYPE> {

	// =================================================================================================================
	// STATIC FACTORY METHODS
	// =================================================================================================================

	@SuppressWarnings("unchecked")
	public static <DBTYPE extends ChronoDBInternal> MigrationChain<DBTYPE> fromPackage(final String qualifiedPackageName) {
		checkNotNull(qualifiedPackageName, "Precondition violation - argument 'qualifiedPackageName' must not be NULL!");
		try {
			Set<ClassInfo> topLevelClasses = ClassPath.from(Thread.currentThread().getContextClassLoader())
					// get the top level classes of the migration package
					.getTopLevelClasses(qualifiedPackageName);
			Set<Class<? extends ChronosMigration<DBTYPE>>> classes = topLevelClasses
					// stream the class info objects
					.stream()
					// load the classes
					.map(classInfo -> classInfo.load())
					// only consider @Migration classes
					.filter(clazz -> clazz.getAnnotation(Migration.class) != null)
					// only consider subclasses of ChronosMigration
					.filter(clazz -> ChronosMigration.class.isAssignableFrom(clazz))
					// cast the classes to subclass of chronos migration
					.map(clazz -> (Class<? extends ChronosMigration<DBTYPE>>) clazz)
					// collect them in a set
					.collect(Collectors.toSet());
			return new MigrationChainImpl<>(classes);
		} catch (IOException e) {
			throw new ChronosIOException("Failed to scan classpath for ChronosMigration classes! See root cause for details.", e);
		}
	}

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final List<Class<? extends ChronosMigration<DBTYPE>>> classes;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	private MigrationChainImpl(final Collection<Class<? extends ChronosMigration<DBTYPE>>> migrationClasses) {
		checkNotNull(migrationClasses, "Precondition violation - argument 'migrationClasses' must not be NULL!");
		List<Class<? extends ChronosMigration<DBTYPE>>> classes = Lists.newArrayList(migrationClasses);
		classes.sort(MigrationClassComparator.INSTANCE);
		this.classes = Collections.unmodifiableList(classes);
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public Iterator<Class<? extends ChronosMigration<DBTYPE>>> iterator() {
		return this.classes.iterator();
	}

	@Override
	public List<Class<? extends ChronosMigration<DBTYPE>>> getMigrationClasses() {
		return this.classes;
	}

	@Override
	public MigrationChain<DBTYPE> startingAt(final ChronosVersion from) {
		return new MigrationChainImpl<>(this.classes.stream().filter(clazz -> {
			ChronosVersion classFrom = ChronosVersion.parse(clazz.getAnnotation(Migration.class).from());
			if (classFrom.compareTo(from) >= 0) {
				return true;
			} else {
				return false;
			}
		}).collect(Collectors.toList()));
	}

	@Override
	public void execute(final DBTYPE chronoDB) {
		for (Class<? extends ChronosMigration<DBTYPE>> migrationClass : this) {
			// create an instance of the migration
			final ChronosMigration<DBTYPE> migration;
			try {
				migration = migrationClass.getConstructor().newInstance();
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
				throw new IllegalStateException("Failed to instantiate migration class '" + migrationClass.getName() + "'!", e);
			}
			try {
				ChronoLogger.logInfo("Migrating ChronoDB from " + MigrationClassUtil.from(migrationClass) + " to " + MigrationClassUtil.to(migrationClass) + " ...");
				// execute the migration
				migration.execute(chronoDB);

				// TODO we have a small consistency problem here.
				// What if:
				// - the migration succeeds
				// - ... but the process is shut down before the write to the new chronos version succeeds?
				// This could be fixed by demanding that the migrations are idempotent...

				// update the chronos version
				chronoDB.updateChronosVersionTo(MigrationClassUtil.to(migrationClass));
				ChronoLogger.logInfo("Migration of ChronoDB from " + MigrationClassUtil.from(migrationClass) + " to " + MigrationClassUtil.to(migrationClass) + " completed successfully.");
			} catch (Throwable t) {
				throw new IllegalStateException("Failed to execute migration of class '" + migrationClass.getName() + "'!", t);
			}
		}
	}

}
