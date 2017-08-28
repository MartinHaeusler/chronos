package org.chronos.chronodb.test.migration.chainA;

import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.migration.ChronosMigration;
import org.chronos.chronodb.internal.api.migration.annotations.Migration;

@Migration(from = "0.5.0", to = "0.5.1")
public class MigrationA1 implements ChronosMigration<ChronoDBInternal> {

	@Override
	public void execute(final ChronoDBInternal chronoDB) {

	}

}
