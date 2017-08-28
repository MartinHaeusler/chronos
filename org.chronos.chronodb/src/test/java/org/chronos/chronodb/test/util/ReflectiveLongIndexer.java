package org.chronos.chronodb.test.util;

import java.util.Set;

import org.chronos.chronodb.api.indexing.LongIndexer;
import org.chronos.common.util.ReflectionUtils;

import com.google.common.collect.Sets;

public class ReflectiveLongIndexer extends ReflectiveIndexer<Long> implements LongIndexer {

	public ReflectiveLongIndexer() {
		// default constructor for serialization
	}

	public ReflectiveLongIndexer(final Class<?> clazz, final String fieldName) {
		super(clazz, fieldName);
	}

	@Override
	public Set<Long> getIndexValues(final Object object) {
		Object fieldValue = this.getFieldValue(object);
		Set<Long> resultSet = Sets.newHashSet();
		if (fieldValue instanceof Iterable) {
			for (Object element : (Iterable<?>) fieldValue) {
				if (ReflectionUtils.isLongCompatible(element)) {
					resultSet.add(ReflectionUtils.asLong(element));
				}
			}
		} else {
			if (ReflectionUtils.isLongCompatible(fieldValue)) {
				resultSet.add(ReflectionUtils.asLong(fieldValue));
			}
		}
		return resultSet;
	}

}