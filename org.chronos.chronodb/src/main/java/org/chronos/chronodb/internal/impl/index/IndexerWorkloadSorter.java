package org.chronos.chronodb.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.key.ChronoIdentifier;

import com.google.common.collect.Lists;

public class IndexerWorkloadSorter {

	public static List<Pair<ChronoIdentifier, Pair<Object, Object>>> sort(final Map<ChronoIdentifier, Pair<Object, Object>> identifierToValue) {
		checkNotNull(identifierToValue, "Precondition violation - argument 'identifierToValue' must not be NULL!");
		List<Pair<ChronoIdentifier, Pair<Object, Object>>> resultList = Lists.newArrayList();
		for (Entry<ChronoIdentifier, Pair<Object, Object>> entry : identifierToValue.entrySet()) {
			resultList.add(Pair.of(entry.getKey(), entry.getValue()));
		}
		Collections.sort(resultList, new WorkloadComparator());
		return resultList;
	}

	private static class WorkloadComparator implements Comparator<Pair<ChronoIdentifier, Pair<Object, Object>>> {

		@Override
		public int compare(final Pair<ChronoIdentifier, Pair<Object, Object>> o1, final Pair<ChronoIdentifier, Pair<Object, Object>> o2) {
			if (o1 == null && o2 == null) {
				return 0;
			} else if (o1 != null && o2 == null) {
				return 1;
			} else if (o1 == null && o2 != null) {
				return -1;
			}
			ChronoIdentifier key1 = o1.getKey();
			ChronoIdentifier key2 = o2.getKey();
			// order by timestamp
			int timestampCompare = Long.compare(key1.getTimestamp(), key2.getTimestamp());
			if (timestampCompare != 0) {
				return timestampCompare;
			}
			// then order by branch
			int branchCompare = key1.getBranchName().compareTo(key2.getBranchName());
			if (branchCompare != 0) {
				return branchCompare;
			}
			// then order by keyspace
			int keyspaceCompare = key1.getKeyspace().compareTo(key2.getKeyspace());
			if (keyspaceCompare != 0) {
				return keyspaceCompare;
			}
			// in all other cases, order by key
			return key1.getKey().compareTo(key2.getKey());
		}

	}
}
