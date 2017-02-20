package org.chronos.chronograph.internal.impl.dumpformat.vertexproperty;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.chronos.chronodb.api.dump.ChronoConverter;
import org.chronos.chronograph.internal.impl.dumpformat.GraphDumpFormat;
import org.chronos.chronograph.internal.impl.dumpformat.property.AbstractPropertyDump;
import org.chronos.chronograph.internal.impl.dumpformat.property.PlainPropertyDump;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record.VertexPropertyRecord;

import com.google.common.collect.Maps;

public class VertexPlainPropertyDump extends PlainPropertyDump implements VertexPropertyDump {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private String recordId;
	private Map<String, AbstractPropertyDump> properties;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected VertexPlainPropertyDump() {
		// serialization constructor
	}

	public VertexPlainPropertyDump(final VertexPropertyRecord vpr) {
		this(vpr, null);
	}

	public VertexPlainPropertyDump(final VertexPropertyRecord vpr, final ChronoConverter<?, ?> converter) {
		super(vpr, converter);
		this.recordId = vpr.getId();
		this.properties = Maps.newHashMap();
		for (Entry<String, PropertyRecord> entry : vpr.getProperties().entrySet()) {
			String key = entry.getKey();
			PropertyRecord pRecord = entry.getValue();
			AbstractPropertyDump pDump = GraphDumpFormat.convertPropertyRecordToDumpFormat(pRecord);
			this.properties.put(key, pDump);
		}
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public String getRecordId() {
		return this.recordId;
	}

	@Override
	public Map<String, AbstractPropertyDump> getProperties() {
		return Collections.unmodifiableMap(this.properties);
	}
}
