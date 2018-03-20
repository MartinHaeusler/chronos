package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.api.key.QualifiedKey;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

public class ChangeSet {

	private final Map<String, Map<String, Object>> keyspaceToKeyToValue = Maps.newHashMap();
	private final Map<ChronoIdentifier, Pair<Object, Object>> entriesToIndex = Maps.newHashMap();

	public void addEntry(final String keyspace, final String key, final Object value) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		Map<String, Object> keyspaceMap = this.keyspaceToKeyToValue.get(keyspace);
		if (keyspaceMap == null) {
			keyspaceMap = Maps.newHashMap();
			this.keyspaceToKeyToValue.put(keyspace, keyspaceMap);
		}
		keyspaceMap.put(key, value);
	}

	public void addEntryToIndex(final ChronoIdentifier identifier, final Object oldValue, final Object newValue) {
		checkNotNull(identifier, "Precondition violation - argument 'identifier' must not be NULL!");
		this.entriesToIndex.put(identifier, Pair.of(oldValue, newValue));
	}

	public Iterable<Entry<String, Map<String, byte[]>>> getSerializedEntriesByKeyspace(
			final Function<Object, byte[]> serializer) {
		Set<Entry<String, Map<String, Object>>> set = this.keyspaceToKeyToValue.entrySet();
		return Iterables.transform(set, entry -> {
			String keyspace = entry.getKey();
			Map<String, Object> contents = entry.getValue();
			Map<String, byte[]> serialContents = Maps.transformValues(contents, value -> {
				if (value == null) {
					return null;
				} else {
					return serializer.apply(value);
				}
			});
			return Pair.of(keyspace, serialContents);
		});
	}

	public Set<QualifiedKey> getModifiedKeys() {
		return this.entriesToIndex.keySet().stream().map(id -> QualifiedKey.create(id.getKeyspace(), id.getKey()))
				.collect(Collectors.toSet());
	}

	public Map<ChronoIdentifier, Pair<Object, Object>> getEntriesToIndex() {
		return Collections.unmodifiableMap(this.entriesToIndex);
	}

	public Map<String, Map<String, Object>> getEntriesByKeyspace() {
		return Collections.unmodifiableMap(this.keyspaceToKeyToValue);
	}

	public Set<String> getModifiedKeyspaces() {
		return Collections.unmodifiableSet(this.keyspaceToKeyToValue.keySet());
	}

}
