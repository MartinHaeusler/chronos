package org.chronos.chronograph.internal.impl.index;

import java.util.Set;

import org.chronos.chronodb.api.indexing.DoubleIndexer;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.common.annotation.PersistentClass;

@PersistentClass("kryo")
public class VertexRecordDoubleIndexer2 extends VertexRecordPropertyIndexer2<Double> implements DoubleIndexer {

	protected VertexRecordDoubleIndexer2() {
		// default constructor for serialization
	}

	public VertexRecordDoubleIndexer2(final String propertyName) {
		super(propertyName);
	}

	@Override
	protected Set<Double> getIndexValuesInternal(final PropertyRecord record) {
		return GraphIndexingUtils.getDoubleIndexValues(record);
	}

}
