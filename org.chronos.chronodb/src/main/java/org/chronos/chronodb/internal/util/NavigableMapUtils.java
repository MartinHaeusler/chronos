package org.chronos.chronodb.internal.util;

import static com.google.common.base.Preconditions.*;

import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import com.google.common.collect.Lists;

public class NavigableMapUtils {

	public static <K extends Comparable<K>, V> List<Entry<K, V>> entriesAround(final NavigableMap<K, V> map, final K key, final int count) {
		checkNotNull(map, "Precondition violation - argument 'map' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		NavigableMap<K, V> smallerMap = map.headMap(key, false).descendingMap();
		NavigableMap<K, V> largerMap = map.tailMap(key, true);
		List<Entry<K, V>> resultList = Lists.newArrayList();
		int sizeLower = count / 2;
		int sizeLarger = count - sizeLower;
		if (smallerMap.size() < sizeLower) {
			if (largerMap.size() < sizeLarger) {
				// we don't have enough entries anyways; throw everything into the list
				for (Entry<K, V> binaryEntry : smallerMap.entrySet()) {
					resultList.add(binaryEntry);
				}
				for (Entry<K, V> binaryEntry : largerMap.entrySet()) {
					resultList.add(binaryEntry);
				}
			} else {
				// we don't have enough entries on the "smaller" side, but enough entries
				// on the other side.
				// take everything from the smaller side
				for (Entry<K, V> binaryEntry : smallerMap.entrySet()) {
					resultList.add(binaryEntry);
				}
				// take the remaining entries from the larger side (if we have enough)
				int limit = count - smallerMap.size();
				int added = 0;
				for (Entry<K, V> binaryEntry : largerMap.entrySet()) {
					if (added >= limit) {
						break;
					}
					resultList.add(binaryEntry);
					added++;
				}
			}
		} else {
			if (largerMap.size() < sizeLarger) {
				// we don't have enough entries on the "larger" side, but enough entries on
				// the other side.
				// take everything from the larger side
				for (Entry<K, V> binaryEntry : largerMap.entrySet()) {
					resultList.add(binaryEntry);
				}
				// take the remaining entries from the smaller side (if we have enough)
				int limit = count - largerMap.size();
				int added = 0;
				for (Entry<K, V> binaryEntry : smallerMap.entrySet()) {
					if (added >= limit) {
						break;
					}
					resultList.add(binaryEntry);
					added++;
				}
			} else {
				// enough entries on both sides
				int added = 0;
				for (Entry<K, V> binaryEntry : smallerMap.entrySet()) {
					if (added >= sizeLower) {
						break;
					}
					resultList.add(binaryEntry);
					added++;
				}
				added = 0;
				for (Entry<K, V> binaryEntry : largerMap.entrySet()) {
					if (added >= sizeLarger) {
						break;
					}
					resultList.add(binaryEntry);
					added++;
				}
			}
		}
		return resultList;
	}

}
