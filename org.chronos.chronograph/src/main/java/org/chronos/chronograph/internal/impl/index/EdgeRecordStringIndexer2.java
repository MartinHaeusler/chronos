package org.chronos.chronograph.internal.impl.index;

import java.util.Set;

import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;

public class EdgeRecordStringIndexer2 extends EdgeRecordPropertyIndexer2<String> implements StringIndexer {

	protected EdgeRecordStringIndexer2() {
		// default constructor for serialization
	}

	protected EdgeRecordStringIndexer2(final String propertyName) {
		super(propertyName);
	}

	@Override
	protected Set<String> getIndexValuesInternal(final PropertyRecord record) {
		return GraphIndexingUtils.getStringIndexValues(record);
	}

}
