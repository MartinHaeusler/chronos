package org.chronos.chronodb.internal.impl.migration;

import java.util.Comparator;

import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.migration.ChronosMigration;
import org.chronos.chronodb.internal.api.migration.annotations.Migration;
import org.chronos.common.version.ChronosVersion;

public final class MigrationClassComparator implements Comparator<Class<? extends ChronosMigration<? extends ChronoDBInternal>>> {

	public static MigrationClassComparator INSTANCE = new MigrationClassComparator();

	private MigrationClassComparator() {
		// should be a singleton, thus private constructor
	}

	@Override
	public int compare(final Class<? extends ChronosMigration<? extends ChronoDBInternal>> o1, final Class<? extends ChronosMigration<? extends ChronoDBInternal>> o2) {
		if (o1 == null && o2 == null) {
			return 0;
		} else if (o1 != null && o2 == null) {
			return 1;
		} else if (o1 == null && o2 != null) {
			return -1;
		}
		Migration annotation1 = o1.getAnnotation(Migration.class);
		Migration annotation2 = o2.getAnnotation(Migration.class);
		ChronosVersion from1 = ChronosVersion.parse(annotation1.from());
		ChronosVersion to1 = ChronosVersion.parse(annotation1.to());
		ChronosVersion from2 = ChronosVersion.parse(annotation2.from());
		ChronosVersion to2 = ChronosVersion.parse(annotation2.to());
		if (from1.isGreaterThan(to1)) {
			throw new IllegalStateException("Migration annotation error on class '" + o1.getName() + "': cannot migrate from '" + from1 + "' to '" + to1 + "' - 'to' is smaller than or equal to 'from'!");
		}
		if (from2.isGreaterThan(to2)) {
			throw new IllegalStateException("Migration annotation error on class '" + o2.getName() + "': cannot migrate from '" + from2 + "' to '" + to2 + "' - 'to' is smaller than or equal to 'from'!");
		}
		if (from1.equals(from2)) {
			throw new IllegalStateException("Migration annotation error: classes '" + o1.getName() + "' and '" + o2.getName() + "' specify the same 'from' version: '" + from1 + "'.");
		}
		if (to1.equals(to2)) {
			throw new IllegalStateException("Migration annotation error: classes '" + o1.getName() + "' and '" + o2.getName() + "' specify the same 'to' version: '" + to1 + "'.");
		}
		// overlap checks
		if (from1.isSmallerThan(from2) && to1.isGreaterThan(to2)) {
			throw new IllegalStateException("Migration annotation error: classes '" + o1.getName() + "' and '" + o2.getName() + "' have overlapping version ranges!");
		}
		if (from2.isSmallerThan(from1) && to2.isGreaterThan(to1)) {
			throw new IllegalStateException("Migration annotation error: classes '" + o1.getName() + "' and '" + o2.getName() + "' have overlapping version ranges!");
		}
		if (from1.isGreaterThan(from2) && to1.isGreaterThanOrEqualTo(from2)) {
			return 1;
		}
		if (from1.isSmallerThan(from2) && to1.isSmallerThanOrEqualTo(from2)) {
			return -1;
		}
		throw new IllegalStateException("Failed to order migration classes '" + o1.getName() + "' (from: '" + from1 + "' to '" + to1 + "') and '" + o2.getName() + "' (from: '" + from2 + "' to '" + to2 + "')!");
	}

}
