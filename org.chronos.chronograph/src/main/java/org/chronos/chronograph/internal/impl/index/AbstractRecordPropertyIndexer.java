package org.chronos.chronograph.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronograph.internal.impl.structure.record.ElementRecord;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.common.annotation.PersistentClass;

import com.google.common.collect.Sets;

/**
 * A base class for all indexers working on {@link ElementRecord}s.
 *
 * @deprecated Superseded by {@link AbstractRecordPropertyIndexer2}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
@Deprecated
@PersistentClass("kryo")
public abstract class AbstractRecordPropertyIndexer implements StringIndexer {

	protected String propertyName;

	protected AbstractRecordPropertyIndexer() {
		// default constructor for serialization
	}

	protected AbstractRecordPropertyIndexer(final String propertyName) {
		checkNotNull(propertyName, "Precondition violation - argument 'propertyName' must not be NULL!");
		this.propertyName = propertyName;
	}

	protected Set<String> getIndexValue(final Optional<? extends PropertyRecord> maybePropertyRecord) {
		if (maybePropertyRecord.isPresent() == false) {
			// the vertex doesn't have the property; nothing to index
			return Collections.emptySet();
		} else {
			PropertyRecord property = maybePropertyRecord.get();
			Object value = property.getValue();
			if (value == null) {
				// this should actually never happen, just a safety measure
				return Collections.emptySet();
			}
			if (value instanceof Iterable) {
				// multiplicity-many property
				Iterable<?> iterable = (Iterable<?>) value;
				Set<String> indexedValues = Sets.newHashSet();
				for (Object element : iterable) {
					indexedValues.add(this.convertToString(element));
				}
				return Collections.unmodifiableSet(indexedValues);
			} else {
				// multiplicity-one property
				return Collections.singleton(this.convertToString(value));
			}
		}
	}

	protected String convertToString(final Object element) {
		String string = String.valueOf(element);
		return string;
	}

}
