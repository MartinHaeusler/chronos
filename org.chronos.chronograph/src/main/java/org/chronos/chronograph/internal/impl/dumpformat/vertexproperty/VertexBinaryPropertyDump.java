package org.chronos.chronograph.internal.impl.dumpformat.vertexproperty;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.chronos.chronograph.internal.impl.dumpformat.GraphDumpFormat;
import org.chronos.chronograph.internal.impl.dumpformat.property.AbstractPropertyDump;
import org.chronos.chronograph.internal.impl.dumpformat.property.BinaryPropertyDump;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record.VertexPropertyRecord;

import com.google.common.collect.Maps;

public class VertexBinaryPropertyDump extends BinaryPropertyDump implements VertexPropertyDump {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private String recordId;
	private Map<String, AbstractPropertyDump> properties;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected VertexBinaryPropertyDump() {
		// serialization constructor
	}

	public VertexBinaryPropertyDump(final VertexPropertyRecord vpr) {
		super(vpr);
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
