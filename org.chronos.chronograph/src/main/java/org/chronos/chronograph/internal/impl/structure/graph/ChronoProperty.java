package org.chronos.chronograph.internal.impl.structure.graph;

import static com.google.common.base.Preconditions.*;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.chronograph.internal.impl.transaction.GraphTransactionContext;

public class ChronoProperty<V> implements Property<V> {

	private final ChronoElement owner;
	private final String key;
	private V value;

	private boolean removed;

	public ChronoProperty(final ChronoElement owner, final String key, final V value) {
		this(owner, key, value, false);
	}

	public ChronoProperty(final ChronoElement owner, final String key, final V value, final boolean silent) {
		this.owner = owner;
		this.key = key;
		this.set(value, silent);
	}

	@Override
	public String key() {
		return this.key;
	}

	@Override
	public V value() throws NoSuchElementException {
		if (this.removed) {
			throw new NoSuchElementException("Property '" + this.key + "' was removed and is no longer present!");
		}
		return this.value;
	}

	public void set(final V value) {
		this.set(value, false);
	}

	private void set(final V value, final boolean silent) {
		if (this.removed) {
			throw new NoSuchElementException("Property '" + this.key + "' was removed and is no longer present!");
		}
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		V oldValue = this.value;
		this.value = value;
		if (silent == false) {
			this.getTransactionContext().markPropertyAsModified(this, oldValue);
		}
	}

	@Override
	public boolean isPresent() {
		return this.removed == false;
	}

	@Override
	public ChronoElement element() {
		return this.owner;
	}

	@Override
	public void remove() {
		if (this.removed) {
			return;
		}
		this.removed = true;
		this.owner.removeProperty(this.key);
	}

	public boolean isRemoved() {
		return this.removed;
	}

	public PropertyRecord toRecord() {
		return new PropertyRecord(this.key, this.value);
	}

	@Override
	public int hashCode() {
		return ElementHelper.hashCode(this);
	}

	@Override
	public boolean equals(final Object obj) {
		return ElementHelper.areEqual(this, obj);
	}

	@Override
	public String toString() {
		return StringFactory.propertyString(this);
	}

	protected ChronoGraphTransaction getGraphTransaction() {
		return this.owner.graph().tx().getCurrentTransaction();
	}

	protected GraphTransactionContext getTransactionContext() {
		return this.getGraphTransaction().getContext();
	}

}
