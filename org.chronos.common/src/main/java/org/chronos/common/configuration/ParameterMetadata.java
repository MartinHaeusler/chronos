package org.chronos.common.configuration;

import static com.google.common.base.Preconditions.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.chronos.common.configuration.annotation.EnumFactoryMethod;
import org.chronos.common.configuration.annotation.IgnoredIf;
import org.chronos.common.configuration.annotation.Namespace;
import org.chronos.common.configuration.annotation.Parameter;
import org.chronos.common.configuration.annotation.RequiredIf;
import org.chronos.common.configuration.annotation.ValueAlias;
import org.chronos.common.configuration.annotation.ValueConverter;
import org.chronos.common.util.ReflectionUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ParameterMetadata {

	private final Field field;

	public ParameterMetadata(final Field parameterField) {
		checkNotNull(parameterField, "Precondition violation - argument 'parameterField' must not be NULL!");
		Parameter parameter = parameterField.getAnnotation(Parameter.class);
		checkNotNull(parameter,
				"Precondition violation - argument 'parameterField' must have an @Parameter annotation!");
		this.field = parameterField;
	}

	@SuppressWarnings("unchecked")
	public Class<? extends AbstractConfiguration> getConfigurationClass() {
		return (Class<? extends AbstractConfiguration>) this.field.getDeclaringClass();
	}

	public Parameter getParameterAnnotation() {
		return this.field.getAnnotation(Parameter.class);
	}

	public Class<?> getType() {
		return this.field.getType();
	}

	public boolean isOptional() {
		return this.getParameterAnnotation().optional();
	}

	public String getNamespace() {
		Class<?> configClass = this.getConfigurationClass();
		Namespace namespace = ReflectionUtils.getClassAnnotationRecursively(configClass, Namespace.class);
		if (namespace == null) {
			return null;
		}
		String value = namespace.value();
		if (value == null || value.trim().isEmpty()) {
			return value;
		}
		return value.trim();
	}

	public String getKey() {
		String namespace = this.getNamespace();
		String key = this.getParameterAnnotation().key().trim();
		if (key == null || key.isEmpty()) {
			key = this.field.getName();
		}
		if (namespace == null) {
			return key;
		}
		if (key.startsWith(namespace + '.') && key.length() > namespace.length() + 1) {
			return key;
		}
		return namespace + '.' + key;
	}

	public boolean isConditionallyRequired() {
		RequiredIf[] requiredIfs = this.field.getAnnotationsByType(RequiredIf.class);
		if (requiredIfs != null && requiredIfs.length > 0) {
			return true;
		} else {
			return false;
		}
	}

	public boolean isConditionallyIgnored() {
		IgnoredIf[] ignoredIfs = this.field.getAnnotationsByType(IgnoredIf.class);
		if (ignoredIfs != null && ignoredIfs.length > 0) {
			return true;
		} else {
			return false;
		}
	}

	public Object getValue(final AbstractConfiguration config) {
		checkNotNull(config, "Precondition violation - argument 'config' must not be NULL!");
		try {
			this.field.setAccessible(true);
			return this.field.get(config);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new IllegalArgumentException("Configuration class '" + config.getClass().getName()
					+ "' does not support field '" + this.field.getName() + "'!", e);
		}
	}

	public void setValue(final AbstractConfiguration chronoConfig, final Object parameterValue) {
		checkNotNull(chronoConfig, "Precondition violation - argument 'chronoConfig' must not be NULL!");
		checkNotNull(parameterValue, "Precondition violation - argument 'parameterValue' must not be NULL!");
		try {
			this.field.setAccessible(true);
			this.field.set(chronoConfig, parameterValue);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new IllegalArgumentException(
					"Couldn't set the value of parameter '" + this.field.getName() + "' (" + this.getType().getName()
							+ ") to '" + parameterValue + "' (" + parameterValue.getClass().getName() + ")!",
					e);
		}
	}

	public Set<Condition> getRequriedIfConditions() {
		RequiredIf[] requiredIfs = this.field.getAnnotationsByType(RequiredIf.class);
		if (requiredIfs == null || requiredIfs.length <= 0) {
			return Collections.emptySet();
		} else {
			Set<Condition> conditions = Sets.newHashSet();
			for (RequiredIf requiredIf : requiredIfs) {
				conditions.add(new Condition(requiredIf));
			}
			return Collections.unmodifiableSet(conditions);
		}
	}

	public Set<Condition> getIgnoredIfConditions() {
		IgnoredIf[] ignoredIfs = this.field.getAnnotationsByType(IgnoredIf.class);
		if (ignoredIfs == null || ignoredIfs.length <= 0) {
			return Collections.emptySet();
		} else {
			Set<Condition> conditions = Sets.newHashSet();
			for (IgnoredIf ignoredIf : ignoredIfs) {
				conditions.add(new Condition(ignoredIf));
			}
			return Collections.unmodifiableSet(conditions);
		}
	}

	public boolean isConditionallyRequiredIn(final AbstractConfiguration config) {
		checkNotNull(config, "Precondition violation - argument 'config' must not be NULL!");
		Set<Condition> conditions = this.getRequriedIfConditions();
		for (Condition condition : conditions) {
			if (condition.appliesTo(config)) {
				return true;
			}
		}
		return false;
	}

	public boolean isConditionallyIgnoredIn(final AbstractConfiguration config) {
		checkNotNull(config, "Precondition violation - argument 'config' must not be NULL!");
		Set<Condition> conditions = this.getIgnoredIfConditions();
		for (Condition condition : conditions) {
			if (condition.appliesTo(config)) {
				return true;
			}
		}
		return false;
	}

	public boolean hasValueIn(final AbstractConfiguration chronoConfig) {
		checkNotNull(chronoConfig, "Precondition violation - argument 'chronoConfig' must not be NULL!");
		return this.getValue(chronoConfig) != null;
	}

	public Map<String, String> getValueAliases() {
		ValueAlias[] aliases = this.field.getAnnotationsByType(ValueAlias.class);
		Map<String, String> resultMap = Maps.newHashMap();
		if (aliases == null || aliases.length <= 0) {
			return Collections.unmodifiableMap(resultMap);
		}
		for (ValueAlias alias : aliases) {
			String aliasString = alias.alias();
			String mapToString = alias.mapTo();
			if (resultMap.containsKey(aliasString) && resultMap.get(aliasString).equals(mapToString) == false) {
				throw new IllegalStateException("Defined multiple different aliases for value '" + aliasString
						+ "' on field '" + this.field.getDeclaringClass() + "#" + this.field.getName() + "'!");
			}
			resultMap.put(aliasString, mapToString);
		}
		return Collections.unmodifiableMap(resultMap);
	}

	public Object getValueAliasFor(final Object value) {
		Map<String, String> aliases = this.getValueAliases();
		String alias = aliases.get(value);
		if (alias == null) {
			// no alias present; use original value
			return value;
		} else {
			// use alias
			return alias;
		}
	}

	public ParameterValueConverter getValueParser() {
		ValueConverter annotation = this.field.getAnnotation(ValueConverter.class);
		if (annotation == null) {
			return null;
		}
		Class<? extends ParameterValueConverter> parserClass = annotation.value();
		try {
			Constructor<? extends ParameterValueConverter> constructor = parserClass.getConstructor();
			return constructor.newInstance();
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			throw new IllegalStateException(
					"Unable to instantiate ParameterValueParser class '" + parserClass.getName() + "'!", e);
		}
	}

	public Method getEnumFactoryMethod() {
		if (this.getType().isEnum() == false) {
			return null;
		}
		EnumFactoryMethod annotation = this.field.getAnnotation(EnumFactoryMethod.class);
		if (annotation == null) {
			// no annotation is given, use the default 'valueOf' method that exists implicitly in each enum class
			try {
				return this.getType().getMethod("valueOf", String.class);
			} catch (NoSuchMethodException | SecurityException e) {
				throw new IllegalStateException(
						"Unable to access the 'valueOf' method of enum type '" + this.getType().getName() + "'!", e);
			}
		}
		String factoryMethodName = annotation.value();
		try {
			Method method = this.getType().getMethod(factoryMethodName, String.class);
			if (Modifier.isStatic(method.getModifiers()) == false) {
				throw new IllegalStateException("The specified @EnumFactoryMethod [" + this.getType().getName() + " "
						+ factoryMethodName + "(String)] must be static!");
			}
			if (method.getReturnType().equals(this.getType()) == false) {
				throw new IllegalStateException("The specified @EnumFactoryMethod [" + factoryMethodName
						+ "(String)] must have a return type of " + this.getType().getName() + "!");
			}
			return method;
		} catch (NoSuchMethodException | SecurityException e) {
			throw new IllegalStateException(
					"The specified @EnumFactoryMethod [static " + this.getType().getName() + " " + factoryMethodName
							+ "(String)] does not exist in " + this.getType().getName() + " or is not accessible!",
					e);
		}

	}

	@Override
	public String toString() {
		return "Parameter[key='" + this.getKey() + "', field=" + this.field.getDeclaringClass().getName() + "#"
				+ this.field.getName() + ", type=" + this.getType().getSimpleName() + "]";
	}
}
