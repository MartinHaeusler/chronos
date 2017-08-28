package org.chronos.chronodb.test.util;

import static com.google.common.base.Preconditions.*;

import java.lang.reflect.Field;

import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.common.util.ReflectionUtils;

public abstract class ReflectiveIndexer<T> implements Indexer<T> {

	private Class<?> clazz;
	private String fieldName;

	protected ReflectiveIndexer() {
		// default constructor for serialization
	}

	public ReflectiveIndexer(final Class<?> clazz, final String fieldName) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		checkNotNull(fieldName, "Precondition violation - argument 'fieldName' must not be NULL!");
		this.clazz = clazz;
		this.fieldName = fieldName;
	}

	@Override
	public boolean canIndex(final Object object) {
		if (object == null) {
			return false;
		}
		return this.clazz.isInstance(object);
	}

	protected Object getFieldValue(final Object object) {
		if (object == null) {
			return null;
		}
		try {
			Field field = ReflectionUtils.getDeclaredField(object.getClass(), this.fieldName);
			field.setAccessible(true);
			return field.get(object);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new IllegalStateException("Failed to access field '" + this.fieldName + "' in instance of class '" + object.getClass().getName() + "'!", e);
		}
	}

}