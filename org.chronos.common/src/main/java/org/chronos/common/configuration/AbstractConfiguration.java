package org.chronos.common.configuration;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.chronos.common.configuration.annotation.Parameter;
import org.chronos.common.util.ReflectionUtils;

import com.google.common.collect.Maps;

public abstract class AbstractConfiguration implements ChronosConfiguration {

	private transient Set<ParameterMetadata> metadataCache = null;

	public Set<String> getAllParameterKeys() {
		return this.getMetadata().stream().map(p -> p.getKey()).collect(Collectors.toSet());
	}

	public Set<ParameterMetadata> getMetadata() {
		if (this.metadataCache == null) {
			Set<Field> fields = ReflectionUtils.getAnnotatedFields(this.getClass(), Parameter.class);
			this.metadataCache = fields.stream().map(field -> new ParameterMetadata(field)).collect(Collectors.toSet());
			this.metadataCache = Collections.unmodifiableSet(this.metadataCache);
		}
		return this.metadataCache;
	}

	public ParameterMetadata getMetadataOfParameter(final String parameterKey) {
		Optional<ParameterMetadata> maybeParam = this.getMetadata().stream()
				.filter(p -> p.getKey().equals(parameterKey)).findAny();
		return maybeParam.orElse(null);
	}

	public ParameterMetadata getMetadataOfField(final String fieldName) {
		Set<Field> fields = ReflectionUtils.getAnnotatedFields(this.getClass(), Parameter.class);
		Optional<Field> maybeField = fields.stream().filter(f -> f.getName().equals(fieldName)).findAny();
		if (maybeField.isPresent()) {
			return new ParameterMetadata(maybeField.get());
		} else {
			return null;
		}
	}

	@Override
	public Map<String, Object> asMap() {
		Map<String, Object> map = Maps.newHashMap();
		Set<String> keys = this.getAllParameterKeys();
		for (String key : keys) {
			Object value = this.getMetadataOfParameter(key).getValue(this);
			map.put(key, value);
		}
		return map;
	}

	@Override
	public Configuration asCommonsConfiguration() {
		BaseConfiguration configuration = new BaseConfiguration();
		for (Entry<String, Object> entry : this.asMap().entrySet()) {
			configuration.addProperty(entry.getKey(), entry.getValue());
		}
		return configuration;
	}

}
