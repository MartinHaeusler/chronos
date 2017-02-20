package org.chronos.chronograph.internal.impl.dumpformat;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronograph.internal.impl.structure.record.EdgeTargetRecord;

public class EdgeTargetDump {

	/** The ID of the vertex at the "other end" of the edge. */
	private String otherEndVertexId;
	/** The ID of the edge itself. */
	private String edgeId;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected EdgeTargetDump() {
		// serialization constructor
	}

	public EdgeTargetDump(final EdgeTargetRecord etr) {
		checkNotNull(etr, "Precondition violation - argument 'etr' must not be NULL!");
		this.edgeId = etr.getEdgeId();
		this.otherEndVertexId = etr.getOtherEndVertexId();
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public String getEdgeId() {
		return this.edgeId;
	}

	public String getOtherEndVertexId() {
		return this.otherEndVertexId;
	}

}
