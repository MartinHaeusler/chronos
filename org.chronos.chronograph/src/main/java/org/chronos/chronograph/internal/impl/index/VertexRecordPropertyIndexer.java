package org.chronos.chronograph.internal.impl.index;

import java.util.Optional;
import java.util.Set;

import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record.VertexRecord;

public class VertexRecordPropertyIndexer extends AbstractRecordPropertyIndexer implements ChronoIndexer {

	protected VertexRecordPropertyIndexer() {
		// default constructor for serializer
	}

	public VertexRecordPropertyIndexer(final String propertyName) {
		super(propertyName);
	}

	@Override
	public boolean canIndex(final Object object) {
		return object instanceof VertexRecord;
	}

	@Override
	public Set<String> getIndexValues(final Object object) {
		VertexRecord vertexRecord = (VertexRecord) object;
		Optional<? extends PropertyRecord> maybePropertyRecord = vertexRecord.getProperties().stream()
				.filter(pRecord -> pRecord.getKey().equals(this.propertyName)).findAny();
		return this.getIndexValue(maybePropertyRecord);
	}

}
