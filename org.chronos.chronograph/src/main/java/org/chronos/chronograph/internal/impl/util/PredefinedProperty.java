package org.chronos.chronograph.internal.impl.util;

import static com.google.common.base.Preconditions.*;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

public class PredefinedProperty<E> implements Property<E> {

	// =================================================================================================================
	// STATIC FACTORY
	// =================================================================================================================

	public static <E> PredefinedProperty<E> of(final ChronoElement element, final T property) {
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		checkNotNull(element, "Precondition violation - argument 'element' must not be NULL!");
		return new PredefinedProperty<>(element, property);
	}

	public static boolean existsOn(final ChronoElement element, final T property) {
		checkNotNull(element, "Precondition violation - argument 'element' must not be NULL!");
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		switch (property) {
		case id:
			return true;
		case label:
			return element instanceof Vertex || element instanceof Edge;
		case key:
			return element instanceof VertexProperty;
		case value:
			return element instanceof VertexProperty;
		default:
			throw new UnknownEnumLiteralException(property);
		}
	}

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final ChronoElement owner;
	private final T property;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected PredefinedProperty(final ChronoElement element, final T property) {
		this.owner = element;
		this.property = property;
	}

	@Override
	public String key() {
		return this.property.getAccessor();
	}

	@Override
	@SuppressWarnings("unchecked")
	public E value() throws NoSuchElementException {
		return (E) this.property.apply(this.owner);
	}

	@Override
	public boolean isPresent() {
		return existsOn(this.owner, this.property);
	}

	@Override
	public Element element() {
		return this.owner;
	}

	@Override
	public void remove() {
		throw new IllegalStateException("Cannot remove the predefined property [" + this.property + "]!");
	}

}
