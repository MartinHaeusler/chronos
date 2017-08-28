package org.chronos.chronograph.internal.impl.index;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record.VertexRecord;
import org.chronos.common.annotation.PersistentClass;

/**
 * An indexer working on {@link VertexRecord}s.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 * @param <T>
 *            The type of the values indexed by this indexer.
 */
@PersistentClass("kryo")
public abstract class VertexRecordPropertyIndexer2<T> extends AbstractRecordPropertyIndexer2<T> {

	protected VertexRecordPropertyIndexer2() {
		// default constructor for serializer
	}

	public VertexRecordPropertyIndexer2(final String propertyName) {
		super(propertyName);
	}

	@Override
	public boolean canIndex(final Object object) {
		return object instanceof VertexRecord;
	}

	@Override
	public Set<T> getIndexValues(final Object object) {
		VertexRecord vertexRecord = (VertexRecord) object;
		Optional<? extends PropertyRecord> maybePropertyRecord = vertexRecord.getProperties().stream()
				.filter(pRecord -> pRecord.getKey().equals(this.propertyName)).findAny();
		return maybePropertyRecord.map(this::getIndexValuesInternal).orElse(Collections.emptySet());
	}

	protected abstract Set<T> getIndexValuesInternal(PropertyRecord record);

}
