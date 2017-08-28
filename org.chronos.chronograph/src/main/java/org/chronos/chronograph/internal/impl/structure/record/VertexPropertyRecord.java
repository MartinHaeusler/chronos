package org.chronos.chronograph.internal.impl.structure.record;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.chronos.common.annotation.PersistentClass;

import com.google.common.collect.Maps;

@PersistentClass("kryo")
public final class VertexPropertyRecord extends PropertyRecord {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private String recordId;
	private Map<String, PropertyRecord> properties;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected VertexPropertyRecord() {
		// default constructor for serialization
	}

	public VertexPropertyRecord(final String recordId, final String key, final Object value,
			final Iterator<Property<Object>> properties) {
		super(key, value);
		checkNotNull(recordId, "Precondition violation - argument 'recordId' must not be NULL!");
		checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
		this.recordId = recordId.toString();
		if (properties.hasNext()) {
			this.properties = Maps.newHashMap();
			while (properties.hasNext()) {
				Property<?> property = properties.next();
				String pKey = property.key();
				Object pValue = property.value();
				PropertyRecord pRecord = new PropertyRecord(pKey, pValue);
				this.properties.put(pKey, pRecord);
			}
		}
	}

	public VertexPropertyRecord(final String recordId, final String key, final Object value,
			final Map<String, PropertyRecord> properties) {
		super(key, value);
		checkNotNull(recordId, "Precondition violation - argument 'recordId' must not be NULL!");
		checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
		this.recordId = recordId;
		if (properties.isEmpty() == false) {
			this.properties = Maps.newHashMap();
			this.properties.putAll(properties);
		}
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public String getId() {
		return this.recordId;
	}

	public Map<String, PropertyRecord> getProperties() {
		if (this.properties == null) {
			return Collections.emptyMap();
		}
		return Collections.unmodifiableMap(this.properties);
	}

}
