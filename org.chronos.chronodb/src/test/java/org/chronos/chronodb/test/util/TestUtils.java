package org.chronos.chronodb.test.util;

import static com.google.common.base.Preconditions.*;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestUtils {

	@SuppressWarnings("unchecked")
	public static <T> T createProxy(final Class<T> theInterface, final InvocationHandler handler) {
		checkNotNull(theInterface, "Precondition violation - argument 'theInterface' must not be NULL!");
		checkNotNull(handler, "Precondition violation - argument 'handler' must not be NULL!");
		checkArgument(theInterface.isInterface(), "Precondition violation - argument 'theInterface' must be an interface, not a class!");
		Class<T>[] classes = (Class<T>[]) Array.newInstance(Class.class, 1);
		classes[0] = theInterface;
		return (T) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), classes, handler);
	}

	public static Set<String> randomKeySet(final long size) {
		Set<String> keySet = Sets.newHashSet();
		for (int i = 0; i < size; i++) {
			keySet.add(UUID.randomUUID().toString());
		}
		return keySet;
	}

	public static List<String> randomKeySetAsList(final long size) {
		List<String> keys = Lists.newArrayList();
		for (int i = 0; i < size; i++) {
			keys.add(UUID.randomUUID().toString());
		}
		return keys;
	}

	public static int randomBetween(final int lower, final int upper) {
		if (lower > upper) {
			throw new IllegalArgumentException("lower > upper: " + lower + " > " + upper);
		}
		if (lower == upper) {
			return lower;
		}
		double random = Math.random();
		return (int) (lower + Math.round((upper - lower) * random));
	}

	public static long randomBetween(final long lower, final long upper) {
		if (lower > upper) {
			throw new IllegalArgumentException("lower > upper: " + lower + " > " + upper);
		}
		if (lower == upper) {
			return lower;
		}
		double random = Math.random();
		return lower + Math.round((upper - lower) * random);
	}

	public static double lerp(final double lower, final double upper, final double percent) {
		return lower + percent * (upper - lower);
	}

	public static <T> List<T> generateValuesList(final Supplier<T> factory, final int size) {
		return generateValues(factory, Lists.newArrayList(), size);
	}

	public static <T> Set<T> generateValuesSet(final Supplier<T> factory, final int size) {
		return generateValues(factory, Sets.newHashSet(), size);
	}

	public static <T, C extends Collection<T>> C generateValues(final Supplier<T> factory, final C collection, final int size) {
		for (int i = 0; i < size; i++) {
			T element = factory.get();
			collection.add(element);
		}
		return collection;
	}

	public static <T> T getRandomEntryOf(final List<T> list) {
		if (list == null) {
			throw new NullPointerException("Precondition violation - argument 'list' must not be NULL!");
		}
		if (list.isEmpty()) {
			throw new NoSuchElementException("List is empty, cannot get random entry!");
		}
		int index = randomBetween(0, list.size() - 1);
		return list.get(index);
	}

	public static <T> Set<T> getRandomUniqueEntriesOf(final List<T> list, final int entries) {
		checkNotNull(list, "Precondition violation - argument 'list' must not be NULL!");
		checkArgument(entries >= 0, "Precondition violation - argument 'entries' must not be negative!");
		checkArgument(entries <= list.size(), "Precondition violation - argument 'entries' must not exceed the list size!");
		Set<T> resultSet = Sets.newHashSet();
		for (int i = 0; i < entries; i++) {
			boolean added = false;
			while (!added) {
				T entry = getRandomEntryOf(list);
				added = resultSet.add(entry);
			}
		}
		return resultSet;
	}

}
