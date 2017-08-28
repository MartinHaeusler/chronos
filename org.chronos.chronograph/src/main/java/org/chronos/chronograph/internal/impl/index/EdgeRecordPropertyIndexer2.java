package org.chronos.chronograph.internal.impl.index;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.chronos.chronograph.internal.impl.structure.record.EdgeRecord;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.common.annotation.PersistentClass;

/**
 * An indexer working on {@link EdgeRecord}s.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 * @param <T>
 *            The type of values produced by this indexer.
 */
@PersistentClass("kryo")
public abstract class EdgeRecordPropertyIndexer2<T> extends AbstractRecordPropertyIndexer2<T> {

	protected EdgeRecordPropertyIndexer2() {
		// default constructor for serializer
	}

	public EdgeRecordPropertyIndexer2(final String propertyName) {
		super(propertyName);
	}

	@Override
	public boolean canIndex(final Object object) {
		return object instanceof EdgeRecord;
	}

	@Override
	public Set<T> getIndexValues(final Object object) {
		EdgeRecord vertexRecord = (EdgeRecord) object;
		Optional<? extends PropertyRecord> maybePropertyRecord = vertexRecord.getProperties().stream()
				.filter(pRecord -> pRecord.getKey().equals(this.propertyName)).findAny();
		return maybePropertyRecord.map(this::getIndexValuesInternal).orElse(Collections.emptySet());
	}

	protected abstract Set<T> getIndexValuesInternal(PropertyRecord record);

}
