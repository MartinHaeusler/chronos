package org.chronos.chronograph.internal.impl.dumpformat.converter;

import org.chronos.chronodb.api.dump.ChronoConverter;
import org.chronos.chronograph.internal.impl.dumpformat.EdgeDump;
import org.chronos.chronograph.internal.impl.structure.record.EdgeRecord;

public class EdgeRecordConverter implements ChronoConverter<EdgeRecord, EdgeDump> {

	@Override
	public EdgeDump writeToOutput(final EdgeRecord record) {
		if (record == null) {
			return null;
		}
		return new EdgeDump(record);
	}

	@Override
	public EdgeRecord readFromInput(final EdgeDump dump) {
		if (dump == null) {
			return null;
		}
		return dump.toRecord();
	}

}
