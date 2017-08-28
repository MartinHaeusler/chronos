package org.chronos.chronodb.internal.impl.migration;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.api.migration.ChronosMigration;
import org.chronos.chronodb.internal.api.migration.annotations.Migration;
import org.chronos.common.version.ChronosVersion;

public class MigrationClassUtil {

	public static ChronosVersion from(final Class<? extends ChronosMigration<?>> migrationClass) {
		checkNotNull(migrationClass, "Precondition violation - argument 'migrationClass' must not be NULL!");
		Migration annotation = migrationClass.getAnnotation(Migration.class);
		if (annotation == null) {
			throw new IllegalArgumentException("Failed to read 'Migration' annotation from migration class '" + migrationClass.getName() + "'!");
		}
		return ChronosVersion.parse(annotation.from());
	}

	public static ChronosVersion to(final Class<? extends ChronosMigration<?>> migrationClass) {
		checkNotNull(migrationClass, "Precondition violation - argument 'migrationClass' must not be NULL!");
		Migration annotation = migrationClass.getAnnotation(Migration.class);
		if (annotation == null) {
			throw new IllegalArgumentException("Failed to read 'Migration' annotation from migration class '" + migrationClass.getName() + "'!");
		}
		return ChronosVersion.parse(annotation.to());
	}
}
