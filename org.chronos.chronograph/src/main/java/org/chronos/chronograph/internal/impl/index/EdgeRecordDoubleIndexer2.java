package org.chronos.chronograph.internal.impl.index;

import java.util.Set;

import org.chronos.chronodb.api.indexing.DoubleIndexer;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.common.annotation.PersistentClass;

@PersistentClass("kryo")
public class EdgeRecordDoubleIndexer2 extends EdgeRecordPropertyIndexer2<Double> implements DoubleIndexer {

	protected EdgeRecordDoubleIndexer2() {
		// default constructor for serialization
	}

	public EdgeRecordDoubleIndexer2(final String propertyName) {
		super(propertyName);
	}

	@Override
	protected Set<Double> getIndexValuesInternal(final PropertyRecord record) {
		return GraphIndexingUtils.getDoubleIndexValues(record);
	}

}
