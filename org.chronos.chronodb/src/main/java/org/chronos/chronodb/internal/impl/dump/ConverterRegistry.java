package org.chronos.chronodb.internal.impl.dump;

import static com.google.common.base.Preconditions.*;

import java.util.Map;

import org.chronos.chronodb.api.DumpOption.DefaultConverterOption;
import org.chronos.chronodb.api.dump.ChronoConverter;
import org.chronos.chronodb.api.dump.annotations.ChronosExternalizable;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.util.ReflectionUtils;

import com.google.common.collect.Maps;

public class ConverterRegistry {

	private final Map<Class<?>, ChronoConverter<?, ?>> defaultConverters = Maps.newHashMap();
	private final Map<Class<? extends ChronoConverter<?, ?>>, ChronoConverter<?, ?>> converterCache = Maps.newHashMap();

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public ConverterRegistry(final DumpOptions options) {
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		this.loadDefaultConverters(options);
	}

	public ChronoConverter<?, ?> getConverterForObject(final Object object) {
		if (object == null) {
			return null;
		}
		Class<?> converterClass = this.getAnnotatedConverterClass(object);
		if (converterClass != null) {
			// found a converter annotation, get the matching instance
			return this.getOrCreateConverterWithClass(converterClass);
		}
		if (converterClass == null) {
			// the object itself has no converter, check the default converters
			ChronoConverter<?, ?> converter = this.defaultConverters.get(object.getClass());
			if (converter != null) {
				// found a default converter
				return converter;
			}
		}
		// neither an annotation was present nor a default converter...
		return null;
	}

	public boolean isDefaultConverter(final ChronoConverter<?, ?> converter) {
		if (converter == null) {
			return false;
		}
		return this.defaultConverters.values().contains(converter);
	}

	public ChronoConverter<?, ?> getConverterByClassName(final String converterClassName) {
		try {
			Class<?> converterClass = Class.forName(converterClassName);
			return this.getOrCreateConverterWithClass(converterClass);
		} catch (ClassNotFoundException e) {
			ChronoLogger.logError("Could not find ChronoConverter with class '" + converterClassName + "'!", e);
			return null;
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private void loadDefaultConverters(final DumpOptions options) {
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		for (DefaultConverterOption option : options.getDefaultConverterOptions()) {
			Class<?> modelClass = option.getType();
			ChronoConverter<?, ?> converter = option.getConverter();
			this.defaultConverters.put(modelClass, converter);
			this.registerConverterInCache(converter);
		}
	}

	@SuppressWarnings("unchecked")
	private void registerConverterInCache(final ChronoConverter<?, ?> converter) {
		this.converterCache.put((Class<? extends ChronoConverter<?, ?>>) converter.getClass(), converter);
	}

	private ChronoConverter<?, ?> getOrCreateConverterWithClass(final Class<?> converterClass) {
		checkNotNull(converterClass, "Precondition violation - argument 'converterClass' must not be NULL!");
		// first, check if it's in the cache
		ChronoConverter<?, ?> cachedConverter = this.converterCache.get(converterClass);
		if (cachedConverter != null) {
			return cachedConverter;
		}
		// we didn't find it in the cache, instantiate it, cache it and return it
		try {
			ChronoConverter<?, ?> converter = (ChronoConverter<?, ?>) ReflectionUtils.instantiate(converterClass);
			this.registerConverterInCache(converter);
			return converter;
		} catch (Exception e) {
			ChronoLogger
					.logWarning("Could not instantiate Plain-Text-Converter class '" + converterClass.getName() + "'.");
			return null;
		}
	}

	private Class<?> getAnnotatedConverterClass(final Object value) {
		ChronosExternalizable annotation = value.getClass().getAnnotation(ChronosExternalizable.class);
		if (annotation == null) {
			return null;
		}
		Class<? extends ChronoConverter<?, ?>> converterClass = annotation.converterClass();
		return converterClass;
	}

}
