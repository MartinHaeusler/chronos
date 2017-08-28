package org.chronos.chronograph.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Set;

import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.common.util.ReflectionUtils;

import com.google.common.collect.Sets;

public class GraphIndexingUtils {

	public static Set<String> getStringIndexValues(final PropertyRecord record) {
		checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
		Object value = record.getValue();
		if (value == null) {
			// this should actually never happen, just a safety measure
			return Collections.emptySet();
		}
		if (value instanceof Iterable) {
			// multiplicity-many property
			Iterable<?> iterable = (Iterable<?>) value;
			Set<String> indexedValues = Sets.newHashSet();
			for (Object element : iterable) {
				indexedValues.add(String.valueOf(element));
			}
			return Collections.unmodifiableSet(indexedValues);
		} else {
			// multiplicity-one property
			return Collections.singleton(String.valueOf(value));
		}
	}

	public static Set<Long> getLongIndexValues(final PropertyRecord record) {
		checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
		Object value = record.getValue();
		if (value == null) {
			// this should actually never happen, just a safety measure
			return Collections.emptySet();
		}
		if (value instanceof Iterable) {
			// multiplicity-many property
			Iterable<?> iterable = (Iterable<?>) value;
			Set<Long> indexedValues = Sets.newHashSet();
			for (Object element : iterable) {
				Long indexValue = asLongIndexValue(element);
				if (indexValue != null) {
					indexedValues.add(indexValue);
				}
			}
			return Collections.unmodifiableSet(indexedValues);
		} else {
			// multiplicity-one property
			Long indexValue = asLongIndexValue(value);
			if (indexValue != null) {
				return Collections.singleton(indexValue);
			} else {
				return Collections.emptySet();
			}
		}
	}

	public static Set<Double> getDoubleIndexValues(final PropertyRecord record) {
		checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
		Object value = record.getValue();
		if (value == null) {
			// this should actually never happen, just a safety measure
			return Collections.emptySet();
		}
		if (value instanceof Iterable) {
			// multiplicity-many property
			Iterable<?> iterable = (Iterable<?>) value;
			Set<Double> indexedValues = Sets.newHashSet();
			for (Object element : iterable) {
				Double indexValue = asDoubleIndexValue(element);
				if (indexValue != null) {
					indexedValues.add(indexValue);
				}
			}
			return Collections.unmodifiableSet(indexedValues);
		} else {
			// multiplicity-one property
			Double indexValue = asDoubleIndexValue(value);
			if (indexValue != null) {
				return Collections.singleton(indexValue);
			} else {
				return Collections.emptySet();
			}
		}
	}

	public static Long asLongIndexValue(final Object singleValue) {
		if (singleValue == null) {
			return null;
		}
		try {
			return ReflectionUtils.asLong(singleValue);
		} catch (Exception e) {
			// conversion failed
			return null;
		}
	}

	public static Double asDoubleIndexValue(final Object singleValue) {
		if (singleValue == null) {
			return null;
		}
		try {
			return ReflectionUtils.asDouble(singleValue);
		} catch (Exception e) {
			// conversion failed
			return null;
		}
	}

}
