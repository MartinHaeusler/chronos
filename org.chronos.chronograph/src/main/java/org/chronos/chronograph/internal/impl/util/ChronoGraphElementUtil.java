package org.chronos.chronograph.internal.impl.util;

import static com.google.common.base.Preconditions.*;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoVertex;

public class ChronoGraphElementUtil {

	private static final String LABEL = Graph.Hidden.hide("label");
	private static final String ID = Graph.Hidden.hide("id");
	private static final String KEY = Graph.Hidden.hide("key");
	private static final String VALUE = Graph.Hidden.hide("value");

	public static boolean isHiddenPropertyKey(final String propertyKey) {
		checkNotNull(propertyKey, "Precondition violation - argument 'propertyKey' must not be NULL!");
		return Graph.Hidden.isHidden(propertyKey);
	}

	public static boolean isLabelProperty(final String propertyKey) {
		checkNotNull(propertyKey, "Precondition violation - argument 'propertyKey' must not be NULL!");
		return LABEL.equals(propertyKey);
	}

	public static boolean isIdProperty(final String propertyKey) {
		checkNotNull(propertyKey, "Precondition violation - argument 'propertyKey' must not be NULL!");
		return ID.equals(propertyKey);
	}

	public static boolean isKeyProperty(final String propertyKey) {
		checkNotNull(propertyKey, "Precondition violation - argument 'propertyKey' must not be NULL!");
		return KEY.equals(propertyKey);
	}

	public static boolean isValueProperty(final String propertyKey) {
		checkNotNull(propertyKey, "Precondition violation - argument 'propertyKey' must not be NULL!");
		return VALUE.equals(propertyKey);
	}

	public static T asSpecialProperty(final String propertyKey) {
		if (isHiddenPropertyKey(propertyKey) == false) {
			return null;
		}
		if (isLabelProperty(propertyKey)) {
			return T.label;
		} else if (isIdProperty(propertyKey)) {
			return T.id;
		} else if (isKeyProperty(propertyKey)) {
			return T.key;
		} else if (isValueProperty(propertyKey)) {
			return T.value;
		} else {
			return null;
		}
	}

	public static <E> PredefinedProperty<E> asPredefinedProperty(final ChronoElement element, final String propertyKey) {
		T specialProperty = asSpecialProperty(propertyKey);
		if (specialProperty == null) {
			return null;
		}
		if (PredefinedProperty.existsOn(element, specialProperty) == false) {
			return null;
		}
		return PredefinedProperty.of(element, specialProperty);
	}

	public static <E> PredefinedVertexProperty<E> asPredefinedVertexProperty(final ChronoVertex vertex, final String propertyKey) {
		T specialProperty = asSpecialProperty(propertyKey);
		if (specialProperty == null) {
			return null;
		}
		if (PredefinedVertexProperty.existsOn(vertex, specialProperty) == false) {
			return null;
		}
		return PredefinedVertexProperty.of(vertex, specialProperty);
	}

}
