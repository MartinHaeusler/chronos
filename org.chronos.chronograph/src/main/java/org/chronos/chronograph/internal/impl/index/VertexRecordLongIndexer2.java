package org.chronos.chronograph.internal.impl.index;

import java.util.Set;

import org.chronos.chronodb.api.indexing.LongIndexer;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.common.annotation.PersistentClass;

@PersistentClass("kryo")
public class VertexRecordLongIndexer2 extends VertexRecordPropertyIndexer2<Long> implements LongIndexer {

	protected VertexRecordLongIndexer2() {
		// default constructor for serialization
	}

	public VertexRecordLongIndexer2(final String propertyName) {
		super(propertyName);
	}

	@Override
	protected Set<Long> getIndexValuesInternal(final PropertyRecord record) {
		return GraphIndexingUtils.getLongIndexValues(record);
	}

}
