package org.chronos.chronodb.internal.impl.dump.base;

import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpEntry;
import org.chronos.chronodb.internal.impl.dump.meta.ChronoDBDumpMetadata;

public interface ChronoDBDump extends Iterable<ChronoDBDumpEntry<?>>, AutoCloseable {

	public ChronoDBDumpMetadata getDumpMetadata();

	@Override
	public void close();

}
