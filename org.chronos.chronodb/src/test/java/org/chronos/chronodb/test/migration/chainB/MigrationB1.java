package org.chronos.chronodb.test.migration.chainB;

import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.migration.ChronosMigration;
import org.chronos.chronodb.internal.api.migration.annotations.Migration;

@Migration(from = "0.5.3", to = "0.5.4")
public class MigrationB1 implements ChronosMigration<ChronoDBInternal> {

	@Override
	public void execute(final ChronoDBInternal chronoDB) {
	}
}
