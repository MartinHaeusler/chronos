package org.chronos.chronograph.internal.impl.util;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

public class PredefinedVertexProperty<E> implements VertexProperty<E> {

	// =================================================================================================================
	// STATIC FACTORY METHODS
	// =================================================================================================================

	public static <E> PredefinedVertexProperty<E> of(final ChronoVertex vertex, final T property) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		return new PredefinedVertexProperty<>(vertex, property);
	}

	public static boolean existsOn(final ChronoVertex vertex, final T property) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		switch (property) {
		case id:
			return true;
		case label:
			return true;
		case key:
			return false;
		case value:
			return false;
		default:
			throw new UnknownEnumLiteralException(property);
		}
	}

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final ChronoVertex vertex;
	private final T property;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected PredefinedVertexProperty(final ChronoVertex vertex, final T property) {
		this.vertex = vertex;
		this.property = property;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public String key() {
		return this.property.getAccessor();
	}

	@Override
	@SuppressWarnings("unchecked")
	public E value() throws NoSuchElementException {
		return (E) this.property.apply(this.vertex);
	}

	@Override
	public boolean isPresent() {
		return existsOn(this.vertex, this.property);
	}

	@Override
	public void remove() {
		throw new IllegalStateException("Cannot remove predefined Vertex Property '" + this.property.getAccessor() + "'!");
	}

	@Override
	public Object id() {
		return null;
	}

	@Override
	public String label() {
		return null;
	}

	@Override
	public ChronoGraph graph() {
		return this.vertex.graph();
	}

	@Override
	public <V> Property<V> property(final String key, final V value) {
		throw new IllegalStateException("Predefined Property '" + this.property.getAccessor() + "' cannot have properties of its own!");
	}

	@Override
	public Vertex element() {
		return this.vertex;
	}

	@Override
	public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
		return Collections.emptyIterator();
	}

}
