package org.chronos.chronograph.internal.impl.index;

import java.util.Optional;
import java.util.Set;

import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronograph.internal.impl.structure.record.EdgeRecord;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;

public class EdgeRecordPropertyIndexer extends AbstractRecordPropertyIndexer implements ChronoIndexer {

	protected EdgeRecordPropertyIndexer() {
		// default constructor for serializer
	}

	public EdgeRecordPropertyIndexer(final String propertyName) {
		super(propertyName);
	}

	@Override
	public boolean canIndex(final Object object) {
		return object instanceof EdgeRecord;
	}

	@Override
	public Set<String> getIndexValues(final Object object) {
		EdgeRecord edgeRecord = (EdgeRecord) object;
		Optional<PropertyRecord> maybePropertyRecord = edgeRecord.getProperties().stream()
				.filter(pRecord -> pRecord.getKey().equals(this.propertyName)).findAny();
		return this.getIndexValue(maybePropertyRecord);
	}

}
