package org.chronos.common.configuration;

import static com.google.common.base.Preconditions.*;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.chronos.common.configuration.annotation.Namespace;
import org.chronos.common.configuration.annotation.Parameter;
import org.chronos.common.exceptions.ChronosConfigurationException;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.util.ReflectionUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ChronosConfigurationUtil {

	public static <T extends AbstractConfiguration> T build(final Configuration apacheConfiguration,
			final Class<T> configurationBeanClass) {
		checkNotNull(apacheConfiguration, "Precondition violation - argument 'apacheConfiguration' must not be NULL!");
		checkNotNull(configurationBeanClass,
				"Precondition violation - argument 'configurationBeanClass' must not be NULL!");
		T configurationBean = null;
		try {
			configurationBean = configurationBeanClass.getConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			throw new IllegalArgumentException("Configuration bean class must have a default constructor!", e);
		}
		injectFieldValues(apacheConfiguration, configurationBean);
		return configurationBean;
	}

	private static void injectFieldValues(final Configuration apacheConfiguration,
			final AbstractConfiguration chronoConfig) {
		checkNotNull(apacheConfiguration, "Precondition violation - argument 'apacheConfiguration' must not be NULL!");
		checkNotNull(chronoConfig, "Precondition violation - argument 'chronoConfig' must not be NULL!");
		Set<Field> fields = ReflectionUtils.getAnnotatedFields(chronoConfig, Parameter.class);
		Map<String, ParameterMetadata> keyToParameter = Maps.newHashMap();
		for (Field field : fields) {
			ParameterMetadata parameter = new ParameterMetadata(field);
			keyToParameter.put(parameter.getKey(), parameter);
		}
		Set<ParameterMetadata> unconfiguredParameters = Sets.newHashSet();
		Set<ParameterMetadata> configuredParameters = Sets.newHashSet();
		for (ParameterMetadata parameter : keyToParameter.values()) {
			String key = parameter.getKey();
			Object defaultValue = parameter.getValue(chronoConfig);
			boolean keyConfigured = apacheConfiguration.containsKey(key);
			if (keyConfigured == false) {
				unconfiguredParameters.add(parameter);
			} else {
				Object configValue = null;
				if (apacheConfiguration.containsKey(key)) {
					configValue = apacheConfiguration.getProperty(key);
				} else {
					configValue = defaultValue;
				}
				// apply aliases (if any)
				configValue = parameter.getValueAliasFor(configValue);
				Object parameterValue = convertToParameterType(configValue, parameter);
				parameter.setValue(chronoConfig, parameterValue);
				configuredParameters.add(parameter);
			}
		}
		// check if all unconfigured parameters either are ignored or have default values
		Set<ParameterMetadata> missingParameters = getMissingParameters(unconfiguredParameters, chronoConfig);
		if (missingParameters.isEmpty() == false) {
			StringBuilder message = new StringBuilder();
			message.append("The following properties are missing in your configuration:\n");
			for (ParameterMetadata missingParameter : missingParameters) {
				message.append(missingParameter.getKey());
				message.append("\n");
			}
			throw new ChronosConfigurationException(message.toString());
		}
		// check if all configured parameters are actually used (and display a warning if that's not the case)
		for (ParameterMetadata parameter : configuredParameters) {
			if (parameter.isConditionallyIgnoredIn(chronoConfig)) {
				ChronoLogger.logWarning("Configuration issue: the parameter '" + parameter.getKey()
						+ "' in your configuration is ignored due to an overriding other parameter."
						+ " Please refer to the documentation.");
			}
		}
		// check if there are some parameters in our namespace that are not mapped at all
		checkForUnknownParametersInNamespace(apacheConfiguration, chronoConfig, keyToParameter);
	}

	@SuppressWarnings({ "rawtypes" })
	private static Object convertToParameterType(final Object setting, final ParameterMetadata parameter) {
		if (parameter.getType().isInstance(setting)) {
			// no conversion neccessary
			return setting;
		}
		ParameterValueConverter parser = parameter.getValueParser();
		if (parser != null) {
			// always use the parser if available
			try {
				return parser.convert(setting);
			} catch (Exception e) {
				throw new ChronosConfigurationException("Custom parser '" + parser.getClass().getName()
						+ "' failed to parse value '" + setting + "' for field '"
						+ parameter.getType().getDeclaringClass() + "#" + parameter.getType().getName() + "'!", e);
			}
		}
		Class<?> parameterType = parameter.getType();
		if (String.class.equals(parameterType)) {
			return setting;
		} else if (Enum.class.isAssignableFrom(parameterType)) {
			Class enumClass = parameterType;
			try {
				Method factoryMethod = parameter.getEnumFactoryMethod();
				return factoryMethod.invoke(null, String.valueOf(setting));
			} catch (IllegalArgumentException | InvocationTargetException e) {
				// given value is no proper enum constant
				throw new ChronosConfigurationException("The value '" + setting + "' for parameter '"
						+ parameter.getKey() + "' is no valid value. Valid values include: "
						+ Arrays.toString(enumClass.getEnumConstants()), e);
			} catch (IllegalAccessException e) {
				throw new ChronosConfigurationException(
						"Unable to access static enum factory method for class '" + enumClass.getName() + "'!", e);
			}
		} else if (ReflectionUtils.isPrimitiveOrWrapperClass(parameterType)) {
			return ReflectionUtils.parsePrimitive(String.valueOf(setting), parameterType);
		} else {
			throw new ChronosConfigurationException("Unable to assign value '" + setting + "' ("
					+ setting.getClass().getName() + ") to " + parameter + "!");
		}
	}

	private static Set<ParameterMetadata> getMissingParameters(final Set<ParameterMetadata> unassigned,
			final AbstractConfiguration chronoConfig) {
		Set<ParameterMetadata> missingParameters = Sets.newHashSet();
		for (ParameterMetadata parameter : unassigned) {
			if (parameter.isOptional()) {
				// parameter is optional; may be left unset
				continue;
			}
			if (parameter.isConditionallyRequired() && parameter.isConditionallyRequiredIn(chronoConfig) == false) {
				// parameter is conditionally required, but our config doesn't require it; may be left unset
				continue;
			}
			if (parameter.hasValueIn(chronoConfig)) {
				// parameter is given by default value
				continue;
			}
			if (parameter.isConditionallyIgnored() && parameter.isConditionallyIgnoredIn(chronoConfig)) {
				// parameter is ignored in our current setup
				continue;
			}
			// parameter is indeed missing
			missingParameters.add(parameter);
		}
		return missingParameters;
	}

	private static void checkForUnknownParametersInNamespace(final Configuration apacheConfiguration,
			final AbstractConfiguration chronoConfig, final Map<String, ParameterMetadata> keyToParameter) {
		String namespace = getNamespace(chronoConfig);
		if (namespace == null) {
			return;
		}
		Iterator<String> keys = apacheConfiguration.getKeys();
		while (keys.hasNext()) {
			String key = keys.next();
			if (key.startsWith(namespace + ".")) {
				ParameterMetadata parameter = keyToParameter.get(key);
				if (parameter == null) {
					// unknown parameter
					ChronoLogger.logWarning("Configuration issue: the parameter '" + key
							+ "' is unknown, but in the namespace '" + namespace + "' - please check the spelling.");
				}
			}
		}
	}

	private static String getNamespace(final AbstractConfiguration configuration) {
		Namespace annotation = ReflectionUtils.getClassAnnotationRecursively(configuration.getClass(), Namespace.class);
		if (annotation == null) {
			return null;
		}
		String namespace = annotation.value().trim();
		if (namespace == null || namespace.isEmpty()) {
			return null;
		}
		return namespace;
	}
}
