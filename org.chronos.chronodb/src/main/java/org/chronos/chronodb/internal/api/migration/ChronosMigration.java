package org.chronos.chronodb.internal.api.migration;

import org.chronos.chronodb.internal.api.ChronoDBInternal;

public interface ChronosMigration<DBTYPE extends ChronoDBInternal> {

	public void execute(DBTYPE chronoDB);
}
