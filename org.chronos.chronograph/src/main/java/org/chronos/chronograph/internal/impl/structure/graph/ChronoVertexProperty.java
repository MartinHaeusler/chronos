package org.chronos.chronograph.internal.impl.structure.graph;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.structure.record.VertexPropertyRecord;
import org.chronos.chronograph.internal.impl.util.ChronoGraphElementUtil;
import org.chronos.chronograph.internal.impl.util.PredefinedProperty;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ChronoVertexProperty<V> extends ChronoProperty<V> implements VertexProperty<V>, ChronoElement {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final String id;
	private final Map<String, ChronoProperty<?>> properties;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public ChronoVertexProperty(final ChronoVertexImpl owner, final String id, final String key, final V value) {
		this(owner, id, key, value, false);
	}

	public ChronoVertexProperty(final ChronoVertexImpl owner, final String id, final String key, final V value,
			final boolean silent) {
		super(owner, key, value, silent);
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		this.id = id;
		this.properties = Maps.newHashMap();
	}

	// =================================================================================================================
	// TINKERPOP 3 API
	// =================================================================================================================

	@Override
	public String id() {
		return this.id;
	}

	@Override
	public String label() {
		return this.key();
	}

	@Override
	public ChronoGraph graph() {
		return this.element().graph();
	}

	@Override
	public ChronoVertexImpl element() {
		return (ChronoVertexImpl) super.element();
	}

	@Override
	public <T> ChronoProperty<T> property(final String key, final T value) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		ChronoProperty<T> newProperty = new ChronoProperty<>(this, key, value);
		this.properties.put(key, newProperty);
		this.updateLifecycleStatus(ElementLifecycleStatus.PROPERTY_CHANGED);
		return newProperty;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public <T> Iterator<Property<T>> properties(final String... propertyKeys) {
		Set<Property> matchingProperties = Sets.newHashSet();
		if (propertyKeys == null || propertyKeys.length <= 0) {
			matchingProperties.addAll(this.properties.values());
		} else {
			for (String key : propertyKeys) {
				PredefinedProperty<?> predefinedProperty = ChronoGraphElementUtil.asPredefinedProperty(this, key);
				if (predefinedProperty != null) {
					matchingProperties.add(predefinedProperty);
				}
				Property property = this.properties.get(key);
				if (property != null) {
					matchingProperties.add(property);
				}
			}
		}
		return new PropertiesIterator<>(matchingProperties.iterator());
	}

	@Override
	public long getLoadedAtRollbackCount() {
		throw new UnsupportedOperationException("VertexProperties do not support 'loaded at rollback count'!");
	}

	@Override
	public ChronoGraphTransaction getOwningTransaction() {
		throw new UnsupportedOperationException("VertexProperties do not support 'getOwningTransaction'!");
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	@Override
	public void removeProperty(final String key) {
		this.properties.remove(key);
		this.updateLifecycleStatus(ElementLifecycleStatus.PROPERTY_CHANGED);
	}

	@Override
	public VertexPropertyRecord toRecord() {
		return new VertexPropertyRecord(this.id, this.key(), this.value(), this.properties());
	}

	@Override
	public void updateLifecycleStatus(final ElementLifecycleStatus status) {
		if (status.isDirty()) {
			this.element().updateLifecycleStatus(ElementLifecycleStatus.PROPERTY_CHANGED);
		}
	}

	@Override
	public ElementLifecycleStatus getStatus() {
		if (this.isRemoved()) {
			return ElementLifecycleStatus.REMOVED;
		} else {
			// TODO properties do not separately track their status, using the element status is not entirely correct.
			return this.element().getStatus();
		}
	}

	@Override
	public int hashCode() {
		return ElementHelper.hashCode((Element) this);
	}

	@Override
	public boolean equals(final Object obj) {
		return ElementHelper.areEqual(this, obj);
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private class PropertiesIterator<T> implements Iterator<Property<T>> {

		@SuppressWarnings("rawtypes")
		private Iterator<Property> iter;

		@SuppressWarnings("rawtypes")
		public PropertiesIterator(final Iterator<Property> iter) {
			this.iter = iter;
		}

		@Override
		public boolean hasNext() {
			return this.iter.hasNext();
		}

		@Override
		@SuppressWarnings("unchecked")
		public Property<T> next() {
			Property<?> p = this.iter.next();
			return (Property<T>) p;
		}

	}

}
