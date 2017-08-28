package org.chronos.chronograph.internal.impl.structure.record;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.dump.annotations.ChronosExternalizable;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.dumpformat.converter.EdgeRecordConverter;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoProperty;
import org.chronos.common.annotation.PersistentClass;

import com.google.common.collect.Sets;

/**
 * An {@link EdgeRecord} is the immutable data core of an edge that has been persisted to the database.
 *
 * <p>
 * This is the class that will actually get serialized as the <code>value</code> in {@link ChronoDBTransaction#put(String, Object)}.
 *
 * <p>
 * It is crucial that all instances of this class are to be treated as immutable after their creation, as these instances are potentially shared among threads due to caching mechanisms.
 *
 * <p>
 * The {@link ChronoEdgeImpl} implementation which typically wraps an {@link EdgeRecord} is mutable and contains the transient (i.e. not yet persisted) state of the edge that is specific for the transaction at hand. Upon calling {@link ChronoGraphTransaction#commit()}, the transient state in {@link ChronoEdgeImpl} will be written into a new {@link EdgeRecord} and persisted to the database with a new timestamp (but the same vertex id), provided that the edge has indeed been modified by the user.
 *
 *
 * <p>
 * Note that an {@link EdgeRecord} only contains the id of <b>one</b> end of the edge. The reason is that the edge will always be serialized embedded in an {@link VertexRecord}, and the vertex record keeps track of the incoming and outgoing edges. Therefore, one 'end' of the edge is always given by the containment structure.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
@PersistentClass("kryo")
@ChronosExternalizable(converterClass = EdgeRecordConverter.class)
public final class EdgeRecord implements ElementRecord {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	// note: the only reason why the fields in this class are not declared as "final" is because
	// serialization mechanisms struggle with final fields. All fields are effectively final, and
	// all of their contents are effectively immutable.

	/** The id of this record, in string representation. */
	private String recordId;
	/** The label of the edge stored in this record. */
	private String label;
	/** The ID of the "In-Vertex", i.e. the target vertex, in string representation. */
	private String inVertexId;
	/** The ID of the "Out-Vertex", i.e. the source vertex, in string representation. */
	private String outVertexId;
	/** The set of properties set on this edge. */
	private Set<PropertyRecord> properties;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected EdgeRecord() {
		// default constructor for serialization
	}

	public EdgeRecord(final String id, final String outVertexId, final String label, final String inVertexId,
			final Map<String, ChronoProperty<?>> properties) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		checkNotNull(outVertexId, "Precondition violation - argument 'outVertexId' must not be NULL!");
		ElementHelper.validateLabel(label);
		checkNotNull(inVertexId, "Precondition violation - argument 'inVertexId' must not be NULL!");
		checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
		this.recordId = id;
		this.outVertexId = outVertexId;
		this.label = label;
		this.inVertexId = inVertexId;
		Collection<ChronoProperty<?>> props = properties.values();
		if (props.isEmpty() == false) {
			// we have at least one property
			this.properties = Sets.newHashSet();
			for (ChronoProperty<?> property : props) {
				PropertyRecord pRecord = new PropertyRecord(property.key(), property.value());
				this.properties.add(pRecord);
			}
		}
	}

	public EdgeRecord(final String id, final String outVertexId, final String label, final String inVertexId,
			final Set<PropertyRecord> properties) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		checkNotNull(outVertexId, "Precondition violation - argument 'outVertexId' must not be NULL!");
		ElementHelper.validateLabel(label);
		checkNotNull(inVertexId, "Precondition violation - argument 'inVertexId' must not be NULL!");
		checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
		this.recordId = id;
		this.outVertexId = outVertexId;
		this.label = label;
		this.inVertexId = inVertexId;
		if (properties.isEmpty() == false) {
			this.properties = Sets.newHashSet();
			this.properties.addAll(properties);
		}
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public String getId() {
		return this.recordId;
	}

	public String getOutVertexId() {
		return this.outVertexId;
	}

	public String getInVertexId() {
		return this.inVertexId;
	}

	public String getLabel() {
		return this.label;
	}

	public Set<PropertyRecord> getProperties() {
		if (this.properties == null) {
			return Collections.emptySet();
		} else {
			return Collections.unmodifiableSet(this.properties);
		}
	}

}
