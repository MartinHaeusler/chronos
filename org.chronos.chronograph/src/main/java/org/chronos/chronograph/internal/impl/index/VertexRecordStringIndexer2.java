package org.chronos.chronograph.internal.impl.index;

import java.util.Set;

import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.common.annotation.PersistentClass;

@PersistentClass("kryo")
public class VertexRecordStringIndexer2 extends VertexRecordPropertyIndexer2<String> implements StringIndexer {

	protected VertexRecordStringIndexer2() {
		// default constructor for serialization
	}

	public VertexRecordStringIndexer2(final String propertyName) {
		super(propertyName);
	}

	@Override
	protected Set<String> getIndexValuesInternal(final PropertyRecord record) {
		Set<String> values = GraphIndexingUtils.getStringIndexValues(record);
		return values;
	}

}
