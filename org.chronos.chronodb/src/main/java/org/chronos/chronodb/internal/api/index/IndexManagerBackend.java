package org.chronos.chronodb.internal.api.index;

import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.api.key.QualifiedKey;

import com.google.common.collect.SetMultimap;

public interface IndexManagerBackend {

	// =================================================================================================================
	// INDEXER MANAGEMENT
	// =================================================================================================================

	public SetMultimap<String, ChronoIndexer> loadIndexersFromPersistence();

	public void persistIndexers(SetMultimap<String, ChronoIndexer> indexNameToIndexers);

	public void persistIndexer(String indexName, ChronoIndexer indexer);

	public void deleteIndexAndIndexers(String indexName);

	public void deleteAllIndicesAndIndexers();

	// =================================================================================================================
	// INDEX DIRTY FLAG MANAGEMENT
	// =================================================================================================================

	public Map<String, Boolean> loadIndexStates();

	public void persistIndexDirtyStates(Map<String, Boolean> indexNameToDirtyFlag);

	// =================================================================================================================
	// INDEX DOCUMENT MANAGEMENT
	// =================================================================================================================

	public void rollback(Set<String> branches, long timestamp);

	public void rollback(Set<String> branches, long timestamp, Set<QualifiedKey> keys);

}
