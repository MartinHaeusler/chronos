package org.chronos.chronograph.internal.impl.dumpformat.converter;

import org.chronos.chronodb.api.dump.ChronoConverter;
import org.chronos.chronograph.internal.impl.dumpformat.VertexDump;
import org.chronos.chronograph.internal.impl.structure.record.VertexRecord;

public class VertexRecordConverter implements ChronoConverter<VertexRecord, VertexDump> {

	@Override
	public VertexDump writeToOutput(final VertexRecord record) {
		if (record == null) {
			return null;
		}
		return new VertexDump(record);
	}

	@Override
	public VertexRecord readFromInput(final VertexDump dump) {
		if (dump == null) {
			return null;
		}
		return dump.toRecord();
	}

}
