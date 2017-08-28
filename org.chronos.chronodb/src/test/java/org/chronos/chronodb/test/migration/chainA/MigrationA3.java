package org.chronos.chronodb.test.migration.chainA;

import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.migration.ChronosMigration;
import org.chronos.chronodb.internal.api.migration.annotations.Migration;

@Migration(from = "0.6.4", to = "0.7.0")
public class MigrationA3 implements ChronosMigration<ChronoDBInternal> {

	@Override
	public void execute(final ChronoDBInternal chronoDB) {

	}

}
