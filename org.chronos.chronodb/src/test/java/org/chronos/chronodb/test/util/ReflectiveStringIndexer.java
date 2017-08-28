package org.chronos.chronodb.test.util;

import java.util.Set;

import org.chronos.chronodb.api.indexing.StringIndexer;

import com.google.common.collect.Sets;

public class ReflectiveStringIndexer extends ReflectiveIndexer<String> implements StringIndexer {

	protected ReflectiveStringIndexer() {
		// default constructor for serialization
	}

	public ReflectiveStringIndexer(final Class<?> clazz, final String fieldName) {
		super(clazz, fieldName);
	}

	@Override
	public Set<String> getIndexValues(final Object object) {
		Object fieldValue = this.getFieldValue(object);
		Set<String> resultSet = Sets.newHashSet();
		if (fieldValue instanceof Iterable) {
			for (Object element : (Iterable<?>) fieldValue) {
				if (element instanceof String) {
					resultSet.add((String) element);
				}
			}
		} else {
			if (fieldValue instanceof String) {
				resultSet.add((String) fieldValue);
			}
		}
		return resultSet;
	}

}