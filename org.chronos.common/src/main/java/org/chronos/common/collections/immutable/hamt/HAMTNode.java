package org.chronos.common.collections.immutable.hamt;

import java.util.Iterator;
import java.util.Map.Entry;

public interface HAMTNode<K, V> {

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public boolean isLeaf();

	public int treeHeight();

	public int entryCount();

	public Iterator<Entry<K, V>> entriesIterator();

	public int childNodeCount();

	public Iterator<HAMTNode<K, V>> childNodeIterator();

	public boolean containsKey(K key);

}