package org.chronos.chronograph.internal.impl.structure.record;

import static com.google.common.base.Preconditions.*;

import org.chronos.common.annotation.PersistentClass;

@PersistentClass("kryo")
public class EdgeTargetRecord implements ElementRecord {

	/** The string representation of the {@link ChronoVertexId} of the vertex at the "other end" of the edge. */
	private String otherEndVertexId;
	/** The string representation of the {@link ChronoEdgeId} of the edge itself. */
	private String edgeId;

	protected EdgeTargetRecord() {
		// default constructor for serialization
	}

	public EdgeTargetRecord(final String edgeId, final String otherEndVertexId) {
		checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
		checkNotNull(otherEndVertexId, "Precondition violation - argument 'otherEndVertexId' must not be NULL!");
		this.edgeId = edgeId.toString();
		this.otherEndVertexId = otherEndVertexId.toString();
	}

	public String getEdgeId() {
		return this.edgeId;
	}

	public String getOtherEndVertexId() {
		return this.otherEndVertexId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.edgeId == null ? 0 : this.edgeId.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		EdgeTargetRecord other = (EdgeTargetRecord) obj;
		if (this.edgeId == null) {
			if (other.edgeId != null) {
				return false;
			}
		} else if (!this.edgeId.equals(other.edgeId)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "EdgeTargetRecord[edgeId='" + this.edgeId + "', otherEndVertexId='" + this.otherEndVertexId + "']";
	}

}
