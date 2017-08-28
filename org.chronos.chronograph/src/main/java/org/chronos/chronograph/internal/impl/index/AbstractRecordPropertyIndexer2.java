package org.chronos.chronograph.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronograph.internal.impl.structure.record.ElementRecord;
import org.chronos.common.annotation.PersistentClass;

/**
 * A base class for all indexers working on {@link ElementRecord}s.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 * @param <T>
 *            The type of values produced by this indexer.
 */
@PersistentClass("kryo")
public abstract class AbstractRecordPropertyIndexer2<T> implements Indexer<T> {

	protected String propertyName;

	protected AbstractRecordPropertyIndexer2() {
		// default constructor for serialization
	}

	protected AbstractRecordPropertyIndexer2(final String propertyName) {
		checkNotNull(propertyName, "Precondition violation - argument 'propertyName' must not be NULL!");
		this.propertyName = propertyName;
	}

}
