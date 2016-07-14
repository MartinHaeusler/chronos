package org.chronos.common.util;

import static com.google.common.base.Preconditions.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;

public final class ReflectionUtils {

	private static final Set<Class<?>> WRAPPER_CLASSES;
	private static final Map<Class<?>, Class<?>> WRAPPER_CLASS_TO_PRIMITIVE_CLASS;
	private static final Map<Class<?>, Class<?>> PRIMITIVE_CLASS_TO_WRAPPER_CLASS;

	static {
		BiMap<Class<?>, Class<?>> wrapperToPrimitive = HashBiMap.create();
		wrapperToPrimitive.put(Boolean.class, boolean.class);
		wrapperToPrimitive.put(Byte.class, byte.class);
		wrapperToPrimitive.put(Short.class, short.class);
		wrapperToPrimitive.put(Character.class, char.class);
		wrapperToPrimitive.put(Integer.class, int.class);
		wrapperToPrimitive.put(Long.class, long.class);
		wrapperToPrimitive.put(Float.class, float.class);
		wrapperToPrimitive.put(Double.class, double.class);
		WRAPPER_CLASS_TO_PRIMITIVE_CLASS = Collections.unmodifiableMap(wrapperToPrimitive);
		PRIMITIVE_CLASS_TO_WRAPPER_CLASS = Collections.unmodifiableMap(wrapperToPrimitive.inverse());
		WRAPPER_CLASSES = Collections.unmodifiableSet(wrapperToPrimitive.keySet());
	}

	public static Set<Field> getAllFields(final Object object) {
		checkNotNull(object, "Precondition violation - argument 'object' must not be NULL!");
		return getAllFields(object.getClass());
	}

	public static Set<Field> getAllFields(final Class<?> clazz) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		Set<Field> allFields = Sets.newHashSet();
		Class<?> currentClass = clazz;
		while (true) {
			if (clazz.equals(Object.class)) {
				// we reached the inheritance root
				return Collections.unmodifiableSet(allFields);
			}
			Field[] localFields = currentClass.getDeclaredFields();
			for (Field field : localFields) {
				allFields.add(field);
			}
			currentClass = currentClass.getSuperclass();
		}
	}

	@SafeVarargs
	public static Set<Field> getAnnotatedFields(final Object object, final Class<? extends Annotation>... annotations) {
		checkNotNull(object, "Precondition violation - argument 'object' must not be NULL!");
		return getAnnotatedFields(object.getClass(), annotations);
	}

	public static Set<Field> getAnnotatedFields(final Object object,
			final Collection<Class<? extends Annotation>> annotations) {
		checkNotNull(object, "Precondition violation - argument 'object' must not be NULL!");
		checkNotNull(annotations, "Precondition violation - argument 'annotations' must not be NULL!");
		return getAnnotatedFields(object.getClass(), annotations);
	}

	@SafeVarargs
	public static Set<Field> getAnnotatedFields(final Class<?> clazz,
			final Class<? extends Annotation>... annotations) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		if (annotations == null || annotations.length <= 0) {
			return getAllFields(clazz);
		}
		Set<Class<? extends Annotation>> annotationSet = Sets.newHashSet(annotations);
		return getAnnotatedFields(clazz, annotationSet);
	}

	public static Set<Field> getAnnotatedFields(final Class<?> clazz,
			final Collection<Class<? extends Annotation>> annotations) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		checkNotNull(annotations, "Precondition violation - argument 'annotations' must not be NULL!");
		if (annotations.isEmpty()) {
			return getAllFields(clazz);
		}
		Set<Field> allFields = Sets.newHashSet();
		Class<?> currentClass = clazz;
		while (currentClass.equals(Object.class) == false) {
			Field[] localFields = currentClass.getDeclaredFields();
			for (Field field : localFields) {
				Annotation[] fieldAnnotations = field.getAnnotations();
				for (Annotation annotation : fieldAnnotations) {
					Class<? extends Annotation> annotationClass = annotation.annotationType();
					if (annotations.contains(annotationClass)) {
						allFields.add(field);
						break;
					}
				}
			}
			currentClass = currentClass.getSuperclass();
		}
		return Collections.unmodifiableSet(allFields);
	}

	public static <T extends Annotation> T getClassAnnotationRecursively(final Class<?> clazz,
			final Class<T> annotation) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		checkNotNull(annotation, "Precondition violation - argument 'annotation' must not be NULL!");
		Class<?> currentClass = clazz;
		while (currentClass.equals(Object.class) == false) {
			T annotationInstance = currentClass.getAnnotation(annotation);
			if (annotationInstance != null) {
				return annotationInstance;
			}
			currentClass = currentClass.getSuperclass();
		}
		return null;
	}

	public static boolean isWrapperClass(final Class<?> clazz) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		return WRAPPER_CLASSES.contains(clazz);
	}

	public static boolean isPrimitiveClass(final Class<?> clazz) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		return clazz.isPrimitive();
	}

	public static boolean isPrimitiveOrWrapperClass(final Class<?> clazz) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		return isPrimitiveClass(clazz) || isWrapperClass(clazz);
	}

	public static Class<?> getWrapperClass(final Class<?> clazz) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		if (isPrimitiveClass(clazz)) {
			return PRIMITIVE_CLASS_TO_WRAPPER_CLASS.get(clazz);
		} else {
			return clazz;
		}
	}

	public static Class<?> getPrimitiveClass(final Class<?> clazz) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		if (isWrapperClass(clazz)) {
			return WRAPPER_CLASS_TO_PRIMITIVE_CLASS.get(clazz);
		} else {
			return clazz;
		}
	}

	public static Object parsePrimitive(final String string, final Class<?> clazz) {
		checkNotNull(string, "Precondition violation - argument 'string' must not be NULL!");
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		// eliminate wrapper class, if any
		Class<?> primitiveClass = getPrimitiveClass(clazz);
		if (primitiveClass.isPrimitive() == false) {
			throw new IllegalArgumentException(
					"Cannot parse primitive - the given class is neither primitive nor a wrapper: '" + clazz.getName()
							+ "'!");
		}
		if (primitiveClass.equals(boolean.class)) {
			return Boolean.parseBoolean(string);
		} else if (primitiveClass.equals(byte.class)) {
			return Byte.parseByte(string);
		} else if (primitiveClass.equals(short.class)) {
			return Short.parseShort(string);
		} else if (primitiveClass.equals(char.class)) {
			if (string.length() != 1) {
				throw new NumberFormatException("The string '" + string + "' cannot be converted into a Character!");
			}
			return string.charAt(0);
		} else if (primitiveClass.equals(int.class)) {
			return Integer.parseInt(string);
		} else if (primitiveClass.equals(long.class)) {
			return Long.parseLong(string);
		} else if (primitiveClass.equals(float.class)) {
			return Float.parseFloat(string);
		} else if (primitiveClass.equals(double.class)) {
			return Double.parseDouble(string);
		}
		throw new IllegalArgumentException(
				"Unable to parse primitive - unknown primitive class: '" + primitiveClass.getName() + "'!");
	}

	public static Method getDeclaredMethod(final Class<?> clazz, final String methodName) {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		checkNotNull(methodName, "Precondition violation - argument 'methodName' must not be NULL!");
		Class<?> currentClass = clazz;
		while (currentClass.equals(Object.class) == false) {
			Method[] methods = currentClass.getDeclaredMethods();
			for (Method method : methods) {
				if (method.getName().equals(methodName)) {
					return method;
				}
			}
			currentClass = currentClass.getSuperclass();
		}
		return null;
	}

	public static <T> T instantiate(final Class<T> clazz) throws NoSuchMethodException, SecurityException,
			InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
		Constructor<T> constructor = clazz.getConstructor();
		if (Modifier.isPublic(constructor.getModifiers()) == false) {
			constructor.setAccessible(true);
		}
		return constructor.newInstance();
	}

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	private ReflectionUtils() {
		// do not instantiate
	}

}
