package org.chronos.chronograph.internal.impl.dumpformat;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronograph.internal.impl.dumpformat.property.AbstractPropertyDump;
import org.chronos.chronograph.internal.impl.structure.record.EdgeRecord;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;

import com.google.common.collect.Sets;

public class EdgeDump {

	/** The id of this record. */
	private String recordId;
	/** The label of the edge stored in this record. */
	private String label;
	/** The ID of the "In-Vertex", i.e. the target vertex. */
	private String inVertexId;
	/** The ID of the "Out-Vertex", i.e. the source vertex. */
	private String outVertexId;
	/** The set of properties set on this edge. */
	private Set<AbstractPropertyDump> properties;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected EdgeDump() {
		// serialization constructor
	}

	public EdgeDump(final EdgeRecord record) {
		checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
		this.recordId = record.getId();
		this.label = record.getLabel();
		this.inVertexId = record.getInVertexId();
		this.outVertexId = record.getOutVertexId();
		Set<AbstractPropertyDump> props = record.getProperties().stream()
				.map(pr -> GraphDumpFormat.convertPropertyRecordToDumpFormat(pr)).collect(Collectors.toSet());
		this.properties = Sets.newHashSet();
		this.properties.addAll(props);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public String getRecordId() {
		return this.recordId;
	}

	public String getInVertexId() {
		return this.inVertexId;
	}

	public String getOutVertexId() {
		return this.outVertexId;
	}

	public String getLabel() {
		return this.label;
	}

	public Set<AbstractPropertyDump> getProperties() {
		return Collections.unmodifiableSet(this.properties);
	}

	public EdgeRecord toRecord() {
		Set<PropertyRecord> props = Sets.newHashSet();
		for (AbstractPropertyDump propertyDump : this.properties) {
			props.add(new PropertyRecord(propertyDump.getKey(), propertyDump.getValue()));
		}
		return new EdgeRecord(this.recordId, this.outVertexId, this.label, this.inVertexId, props);
	}

}
