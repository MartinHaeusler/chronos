package org.chronos.chronograph.internal.impl.dumpformat.vertexproperty;

import java.util.Map;

import org.chronos.chronograph.internal.impl.dumpformat.property.AbstractPropertyDump;

public interface VertexPropertyDump {

	public String getRecordId();

	public String getKey();

	public Object getValue();

	public Map<String, AbstractPropertyDump> getProperties();

}
