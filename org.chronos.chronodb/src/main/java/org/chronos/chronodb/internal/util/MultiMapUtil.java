package org.chronos.chronodb.internal.util;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class MultiMapUtil {

	public static <K, V> void put(final Map<K, Set<V>> multimap, final K key, final V value) {
		checkNotNull(multimap, "Precondition violation - argument 'multimap' must not be NULL!");
		Set<V> set = multimap.get(key);
		if (set == null) {
			set = Sets.newHashSet();
			multimap.put(key, set);
		}
		set.add(value);
	}

	public static <K, V> Map<K, Set<V>> copyToMap(final SetMultimap<K, V> multimap) {
		checkNotNull(multimap, "Precondition violation - argument 'multimap' must not be NULL!");
		Map<K, Set<V>> resultMap = Maps.newHashMap();
		for (Entry<K, V> entry : multimap.entries()) {
			K key = entry.getKey();
			V value = entry.getValue();
			Set<V> set = resultMap.get(key);
			if (set == null) {
				set = Sets.newHashSet();
				resultMap.put(key, set);
			}
			set.add(value);
		}
		return resultMap;
	}

	public static <K, V> Map<K, Set<V>> copy(final Map<K, Set<V>> multimap) {
		checkNotNull(multimap, "Precondition violation - argument 'multimap' must not be NULL!");
		Map<K, Set<V>> resultMap = Maps.newHashMap();
		for (Entry<K, Set<V>> entry : multimap.entrySet()) {
			K key = entry.getKey();
			Set<V> values = entry.getValue();
			Set<V> newValues = Sets.newHashSet(values);
			resultMap.put(key, newValues);
		}
		return resultMap;
	}

	public static <K, V> SetMultimap<K, V> copyToMultimap(final Map<K, Set<V>> map) {
		checkNotNull(map, "Precondition violation - argument 'map' must not be NULL!");
		SetMultimap<K, V> resultMap = HashMultimap.create();
		for (Entry<K, Set<V>> entry : map.entrySet()) {
			K key = entry.getKey();
			Set<V> values = entry.getValue();
			for (V value : values) {
				resultMap.put(key, value);
			}
		}
		return resultMap;
	}
}
