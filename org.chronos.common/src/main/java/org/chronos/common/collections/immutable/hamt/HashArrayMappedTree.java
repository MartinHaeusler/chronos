package org.chronos.common.collections.immutable.hamt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.common.collections.immutable.ImmutableMap;
import org.chronos.common.collections.immutable.base.AbstractImmutableCollection;
import org.chronos.common.collections.immutable.base.AbstractImmutableSet;
import org.chronos.common.collections.immutable.base.ImmutableEntry;
import org.chronos.common.collections.util.BitFieldUtil;

import com.google.common.collect.Iterators;

public class HashArrayMappedTree<K, V> implements ImmutableMap<K, V> {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	private static final HashArrayMappedTree<?, ?> EMPTY_TREE = new HashArrayMappedTree<>();

	// =====================================================================================================================
	// STATIC FACTORY METHODS
	// =====================================================================================================================

	@SuppressWarnings("unchecked")
	public static <K, V> HashArrayMappedTree<K, V> create() {
		return (HashArrayMappedTree<K, V>) EMPTY_TREE;
	}

	public static <K, V> HashArrayMappedTree<K, V> create(final Map<K, V> map) {
		if (map == null) {
			throw new NullPointerException("Precondition violation - argument 'map' must not be NULL!");
		}
		HashArrayMappedTree<K, V> tree = create();
		return tree.putAll(map);
	}

	public static <K, V> HashArrayMappedTree<K, V> create(final Iterable<? extends Entry<K, V>> entries) {
		if (entries == null) {
			throw new NullPointerException("Precondition violation - argument 'entries' must not be NULL!");
		}
		Map<K, V> map = new HashMap<>();
		for (Entry<K, V> entry : entries) {
			map.put(entry.getKey(), entry.getValue());
		}
		return create(map);
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private HAMTNode<K, V> rootNode;
	private int size;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected HashArrayMappedTree() {
		// default constructor; should only be used by the static factories or by (de-)serialization facilities.
		this.rootNode = null;
		this.size = 0;
	}

	protected HashArrayMappedTree(final HAMTNode<K, V> rootNode) {
		this();
		if (rootNode == null) {
			throw new NullPointerException("Precondition violation - argument 'rootNode' must not be NULL!");
		}
		this.rootNode = rootNode;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public Iterator<Entry<K, V>> iterator() {
		if (this.rootNode == null) {
			return Collections.emptyIterator();
		}
		if (this.rootNode.isLeaf()) {
			return this.rootNode.entriesIterator();
		}
		return new AMTIterator<>(this);
	}

	@Override
	public V get(final K key) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		HAMTNode<K, V> node = this.getNodeForKey(key);
		if (node != null && node.containsKey(key)) {
			// we found the matching node
			return asLeafNode(node).getValueForKey(key);
		} else {
			// search did not produce a match
			return null;
		}
	}

	@Override
	public boolean containsKey(final K key) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		return this.get(key) != null;
	}

	@Override
	public HashArrayMappedTree<K, V> put(final K key, final V value) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		if (value == null) {
			throw new NullPointerException("Precondition violation - argument 'value' must not be NULL!");
		}
		if (this.rootNode == null) {
			// case 1: we have an empty tree
			HAMTNode<K, V> newRootNode = new HAMTLeafNode<>(key, value);
			HashArrayMappedTree<K, V> newTree = new HashArrayMappedTree<>(newRootNode);
			newTree.size = 1;
			return newTree;
		} else {
			// case 2: we have a non-empty tree. We first need to find the node at which our insertion happens.
			AMTPath<K, V> traversedPath = new AMTPath<>();
			HAMTNode<K, V> currentNode = this.rootNode;
			int depth = 0;
			int hashCode = key.hashCode();
			while (currentNode.isLeaf() == false) {
				// take the next 5 bits from the hash code
				int nodeBits = BitFieldUtil.getBitsAndShiftRight(hashCode, depth * 5, 5);
				HAMTNode<K, V> childNode = asIndexNode(currentNode).findNextChildNode(nodeBits);
				if (childNode == null) {
					// we could not find any more child nodes to follow; we need to insert
					// our new node as a new child of the current node
					break;
				} else {
					// add it to our path and continue on
					traversedPath.addStepFromNodeWithBits(currentNode, nodeBits);
					currentNode = childNode;
					depth++;
					continue;
				}
			}
			// check if our node is a leaf or not
			if (currentNode.isLeaf()) {
				// we are inserting at a leaf node. Check if it contains the same key we want to insert
				if (currentNode.containsKey(key)) {
					// same key, check if the values are equal as well
					if (Objects.equals(asLeafNode(currentNode).getValueForKey(key), value)) {
						// the very same key-value pair we wanted to insert is already present; nothing to do
						return this;
					} else {
						// the key already exists, but is associated with a different value.
						// create a new leaf node with the new value, and clone the traversed nodes
						HAMTNode<K, V> replacementNode = asLeafNode(currentNode)
								.putEntry(ImmutableEntry.of(key, value));
						// traverse the path backwards, replacing the traversed nodes
						HashArrayMappedTree<K, V> newTree = this.clonePathBackToRootAndCreateTree(traversedPath,
								replacementNode);
						// swapping one key-value pair for another one leaves the size untouched
						newTree.size = this.size;
						return newTree;
					}
				} else {
					// the key is not contained. Check if we may add another layer
					if (this.isHashCodeLengthExceeded(depth + 1) == false) {
						// hash code is not yet exhausted; convert the current node into an intermediate
						// node and insert both values at a lower level.
						Entry<K, V> existingEntry = asLeafNode(currentNode).getSingleEntry();
						HAMTNode<K, V> replacementNode = asLeafNode(currentNode).removeEntry(existingEntry.getKey());
						replacementNode = this.insertCollidingEntries(replacementNode, depth,
								ImmutableEntry.of(key, value), existingEntry);
						HashArrayMappedTree<K, V> newTree = null;
						if (traversedPath.isEmpty()) {
							// collision happened at the root, simply replace the entire tree
							newTree = new HashArrayMappedTree<>(replacementNode);
						} else {
							// collision happened somewhere further down the tree
							newTree = this.clonePathBackToRootAndCreateTree(traversedPath, replacementNode);
						}
						newTree.size = this.size + 1;
						return newTree;
					} else {
						// hash code is exhausted, we have a "true" collision. We solve it by storing
						// both colliding keys (along with their values) in the node.
						HAMTNode<K, V> replacementNode = asLeafNode(currentNode)
								.putEntry(ImmutableEntry.of(key, value));
						// traverse the path backwards, replacing the traversed nodes
						HashArrayMappedTree<K, V> newTree = this.clonePathBackToRootAndCreateTree(traversedPath,
								replacementNode);
						newTree.size = this.size + 1;
						return newTree;
					}
				}
			} else {
				// we are inserting a new child at an intermediate node
				HAMTNode<K, V> newChildNode = new HAMTLeafNode<>(key, value);
				// note: we have to reduce the depth by 1 here since we already incremented it in the loop
				int nodeBits = BitFieldUtil.getBitsAndShiftRight(hashCode, depth * 5, 5);
				HAMTNode<K, V> replacementNode = asIndexNode(currentNode).attachChild(nodeBits, newChildNode);
				HashArrayMappedTree<K, V> newTree = this.clonePathBackToRootAndCreateTree(traversedPath,
						replacementNode);
				newTree.size = this.size + 1;
				return newTree;
			}
		}
	}

	@Override
	public HashArrayMappedTree<K, V> putAll(final Map<K, V> map) {
		if (map == null) {
			throw new NullPointerException("Precondition violation - argument 'map' must not be NULL!");
		}
		HashArrayMappedTree<K, V> tree = this;
		for (Entry<K, V> entry : map.entrySet()) {
			K key = entry.getKey();
			V value = entry.getValue();
			tree = tree.put(key, value);
		}
		return tree;
	}

	@Override
	public HashArrayMappedTree<K, V> putAll(final ImmutableMap<K, V> map) {
		if (map == null) {
			throw new NullPointerException("Precondition violation - argument 'map' must not be NULL!");
		}
		return this.putAll(map.asUnmodifiableMap());
	}

	@Override
	public HashArrayMappedTree<K, V> remove(final K key) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		return this.removeInternal(key, null);
	}

	@Override
	public HashArrayMappedTree<K, V> remove(final K key, final V value) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		if (value == null) {
			throw new NullPointerException("Precondition violation - argument 'value' must not be NULL!");
		}
		return this.removeInternal(key, value);
	}

	@Override
	public HashArrayMappedTree<K, V> remove(final Entry<K, V> entry) {
		if (entry == null) {
			throw new NullPointerException("Precondition violation - argument 'entry' must not be NULL!");
		}
		return this.remove(entry.getKey(), entry.getValue());
	}

	@Override
	public HashArrayMappedTree<K, V> removeAll(final Map<K, V> map) {
		if (map == null) {
			throw new NullPointerException("Precondition violation - argument 'map' must not be NULL!");
		}
		HashArrayMappedTree<K, V> resultTree = this;
		for (Entry<K, V> entry : map.entrySet()) {
			resultTree = resultTree.remove(entry);
		}
		return resultTree;
	}

	@Override
	public HashArrayMappedTree<K, V> removeAll(final ImmutableMap<K, V> map) {
		if (map == null) {
			throw new NullPointerException("Precondition violation - argument 'map' must not be NULL!");
		}
		HashArrayMappedTree<K, V> resultTree = this;
		for (Entry<K, V> entry : map) {
			resultTree = resultTree.remove(entry);
		}
		return resultTree;
	}

	@Override
	public HashArrayMappedTree<K, V> removeAll(final Collection<K> keys) {
		if (keys == null) {
			throw new NullPointerException("Precondition violation - argument 'keys' must not be NULL!");
		}
		HashArrayMappedTree<K, V> resultTree = this;
		for (K key : keys) {
			resultTree = resultTree.remove(key);
		}
		return resultTree;
	}

	@Override
	public int size() {
		return this.size;
	}

	@Override
	public Set<K> keySet() {
		return new KeySetView();
	}

	@Override
	public Collection<V> values() {
		return new ValuesView();
	}

	@Override
	public boolean isEmpty() {
		if (this.rootNode == null) {
			return true;
		}
		return this.size <= 0;
	}

	@Override
	public int hashCode() {
		return this.entrySet().hashCode();
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean equals(final Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj instanceof ImmutableMap == false) {
			return false;
		}
		ImmutableMap<K, V> other = (ImmutableMap<K, V>) obj;
		return this.entrySet().equals(other.entrySet());
	}

	public int height() {
		if (this.rootNode == null) {
			return 0;
		}
		return this.rootNode.treeHeight();
	}

	// =====================================================================================================================
	// HELPER METHODS
	// =====================================================================================================================

	private HashArrayMappedTree<K, V> clonePathBackToRootAndCreateTree(final AMTPath<K, V> traversedPath,
			HAMTNode<K, V> replacementNode) {
		for (int i = traversedPath.length() - 1; i >= 0; i--) {
			int childNodeBits = traversedPath.getChildNodeBitsAtStep(i);
			HAMTNode<K, V> node = traversedPath.getNodeAtStep(i);
			replacementNode = asIndexNode(node).replaceChild(childNodeBits, replacementNode);
		}
		// our replacement node is now the root for our new tree
		return new HashArrayMappedTree<K, V>(replacementNode);
	}

	private HAMTNode<K, V> insertCollidingEntries(final HAMTNode<K, V> parentNode, final int depth,
			final Entry<K, V> firstEntry, final Entry<K, V> secondEntry) {
		int firstHashCode = firstEntry.getKey().hashCode();
		int secondHashCode = secondEntry.getKey().hashCode();
		int firstChildBits = BitFieldUtil.getBitsAndShiftRight(firstHashCode, depth * 5, 5);
		int secondChildBits = BitFieldUtil.getBitsAndShiftRight(secondHashCode, depth * 5, 5);
		if (firstChildBits == secondChildBits) {
			// still colliding, check if hash code is exhausted
			if (this.isHashCodeLengthExceeded(depth + 1) == false) {
				// hash code is not exhausted; attach a single child for the (common) child bits
				// and repeat the process
				HAMTNode<K, V> childNode = new HAMTIndexNode<>();
				childNode = this.insertCollidingEntries(childNode, depth + 1, firstEntry, secondEntry);
				HAMTIndexNode<K, V> changedParent = null;
				if (parentNode.isLeaf()) {
					changedParent = new HAMTIndexNode<>();
				} else {
					changedParent = asIndexNode(parentNode);
				}
				changedParent = changedParent.attachChild(firstChildBits, childNode);
				return changedParent;
			} else {
				// hash code is exhausted, we have a "true" collision. We solve it by storing
				// both colliding keys (along with their values) in the node.
				HAMTNode<K, V> changedParent = parentNode;
				if (changedParent.isLeaf() == false) {
					changedParent = new HAMTLeafNode<>();
				}
				changedParent = asLeafNode(changedParent).putEntry(firstEntry);
				changedParent = asLeafNode(changedParent).putEntry(secondEntry);
				return changedParent;
			}
		} else {
			// collision resolved; create one node for each and attach them to the parent
			HAMTNode<K, V> firstNode = new HAMTLeafNode<>(firstEntry);
			HAMTNode<K, V> secondNode = new HAMTLeafNode<>(secondEntry);
			HAMTIndexNode<K, V> changedParent = null;
			if (parentNode.isLeaf()) {
				changedParent = new HAMTIndexNode<>();
			} else {
				changedParent = asIndexNode(parentNode);
			}
			changedParent = asIndexNode(changedParent).attachChild(firstChildBits, firstNode);
			changedParent = asIndexNode(changedParent).attachChild(secondChildBits, secondNode);
			return changedParent;
		}
	}

	private HAMTNode<K, V> getNodeForKey(final K key) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		Pair<AMTPath<K, V>, HAMTNode<K, V>> result = this.getNodeForKeyInternal(key, false);
		return result.getRight();
	}

	private Pair<AMTPath<K, V>, HAMTNode<K, V>> getNodeAndPathForKey(final K key) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		return this.getNodeForKeyInternal(key, true);
	}

	private Pair<AMTPath<K, V>, HAMTNode<K, V>> getNodeForKeyInternal(final K key, final boolean trackPath) {
		if (this.rootNode == null) {
			// tree is empty
			if (trackPath) {
				return Pair.of(new AMTPath<>(), null);
			} else {
				return Pair.of(null, null);
			}
		}
		HAMTNode<K, V> node = this.rootNode;
		AMTPath<K, V> path = null;
		if (trackPath) {
			path = new AMTPath<>();
		}
		int depth = 0;
		int hashCode = key.hashCode();
		while (node.isLeaf() == false) {
			// take the next 5 bits from the hash code
			int nodeBits = BitFieldUtil.getBitsAndShiftRight(hashCode, depth * 5, 5);
			HAMTNode<K, V> childNode = asIndexNode(node).findNextChildNode(nodeBits);
			if (trackPath) {
				path.addStepFromNodeWithBits(node, nodeBits);
			}
			if (childNode == null) {
				// no more children, key is not contained
				node = null;
				break;
			}
			// move down the tree to the next node
			node = childNode;
			depth++;
		}
		// check that the node we ended up in did indeed contain the key, it might be an accidental
		// match due to hash code collision.
		if (node == null || node.containsKey(key) == false) {
			// not found
			return Pair.of(path, null);
		} else {
			// found
			return Pair.of(path, node);
		}
	}

	@SuppressWarnings("unchecked")
	private HashArrayMappedTree<K, V> removeInternal(final K key, final V value) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		Pair<AMTPath<K, V>, HAMTNode<K, V>> searchResult = this.getNodeAndPathForKey(key);
		AMTPath<K, V> path = searchResult.getLeft();
		HAMTNode<K, V> node = searchResult.getRight();
		if (node == null) {
			// the requested key is not contained in the tree, nothing to do; tree remains unmodified
			return this;
		}
		if (value != null && Objects.equals(asLeafNode(node).getValueForKey(key), value) == false) {
			// the key matches, but the value does not, nothing to do; tree remains unmodified
			return this;
		}
		// remove the key-value pair from this node
		HAMTLeafNode<K, V> replacementNode = asLeafNode(node).removeEntry(key);
		if (replacementNode.entryCount() > 0) {
			// the node still has some entries, no need for pruning; do the replacements to the root and return the
			// modified tree
			HashArrayMappedTree<K, V> newTree = this.clonePathBackToRootAndCreateTree(path, replacementNode);
			newTree.size = this.size - 1;
			return newTree;
		} else if (path.isEmpty()) {
			// we removed the last entry of the root
			return (HashArrayMappedTree<K, V>) EMPTY_TREE;
		} else {
			// the node has no children anymore, we can safely discard it. Then we "prune" the tree upwards recursively
			// until we find a node that, after removing the empty child, has another child.
			HAMTNode<K, V> parentNode = null;
			// find the first node up the tree that has 2 children
			while (path.isEmpty() == false) {
				parentNode = path.getNodeAtStep(path.length() - 1);
				if (parentNode.childNodeCount() <= 1) {
					// continue by walking upwards the tree
					path.removeLastStep();
					continue;
				} else {
					// we have found the node with another child, keep that
					break;
				}
			}
			if (path.isEmpty()) {
				// failed to find a parent node with two children, the tree was only a single string of nodes, drop it
				return (HashArrayMappedTree<K, V>) EMPTY_TREE;
			}
			// check where we ended up
			if (parentNode == this.rootNode) {
				if (parentNode.childNodeCount() <= 1) {
					// the entire tree was only a single string of nodes, drop everything and return the empty tree
					return (HashArrayMappedTree<K, V>) EMPTY_TREE;
				} else {
					// we ended up at the root, but there is more than one child, drop the child
					int childBits = path.getChildNodeBitsAtStep(path.length() - 1);
					HAMTNode<K, V> newRootNode = asIndexNode(parentNode).detachChild(childBits);
					HashArrayMappedTree<K, V> newTree = new HashArrayMappedTree<>(newRootNode);
					newTree.size = this.size - 1;
					return newTree;
				}
			} else {
				// we ended up at an intermediate node; detach the child node
				int childBits = path.getChildNodeBitsAtStep(path.length() - 1);
				HAMTNode<K, V> changedNode = asIndexNode(parentNode).detachChild(childBits);
				// we need to shorten the path by 1 (because we replaced the last node, and the path still includes it)
				path.removeLastStep();
				HashArrayMappedTree<K, V> newTree = this.clonePathBackToRootAndCreateTree(path, changedNode);
				newTree.size = this.size - 1;
				return newTree;
			}
		}
	}

	private boolean isHashCodeLengthExceeded(final int depth) {
		// we always consume 5 bits per depth, and hash code in total has 32 bits.
		return depth * 5 + 5 > 32;
	}

	private static <K, V> HAMTLeafNode<K, V> asLeafNode(final HAMTNode<K, V> node) {
		return (HAMTLeafNode<K, V>) node;
	}

	private static <K, V> HAMTIndexNode<K, V> asIndexNode(final HAMTNode<K, V> node) {
		return (HAMTIndexNode<K, V>) node;
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	private static class AMTPath<K, V> {

		private final List<Entry<HAMTNode<K, V>, Integer>> steps = new ArrayList<>();

		public void addStepFromNodeWithBits(final HAMTNode<K, V> node, final int nodeBits) {
			this.steps.add(ImmutableEntry.of(node, nodeBits));
		}

		public boolean isEmpty() {
			return this.steps.isEmpty();
		}

		public int length() {
			return this.steps.size();
		}

		public Integer getChildNodeBitsAtStep(final int stepIndex) {
			return this.steps.get(stepIndex).getValue();
		}

		public HAMTNode<K, V> getNodeAtStep(final int stepIndex) {
			return this.steps.get(stepIndex).getKey();
		}

		public void removeLastStep() {
			this.steps.remove(this.steps.size() - 1);
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("P[");
			String separator = "";
			for (Entry<HAMTNode<K, V>, Integer> step : this.steps) {
				HAMTNode<K, V> node = step.getKey();
				Integer childIndex = step.getValue();
				builder.append(separator);
				separator = "->";
				builder.append(node);
				builder.append("#");
				builder.append(childIndex);
			}
			builder.append("]");
			return builder.toString();
		}

	}

	private static class AMTIterator<K, V> implements Iterator<Entry<K, V>> {

		private final Stack<Iterator<HAMTNode<K, V>>> openIteratorsStack = new Stack<>();

		private Iterator<Entry<K, V>> currentEntryIterator;

		public AMTIterator(final HashArrayMappedTree<K, V> tree) {
			if (tree == null) {
				throw new NullPointerException("Precondition violation - argument 'tree' must not be NULL!");
			}
			this.openIteratorsStack.push(tree.rootNode.childNodeIterator());
			this.currentEntryIterator = null;
			this.moveToNextEntryIteratorIfNecessary();
		}

		private void moveToNextEntryIteratorIfNecessary() {
			if (this.hasNext()) {
				// current array iterator is not done yet
				return;
			}
			while (this.openIteratorsStack.isEmpty() == false) {
				Iterator<HAMTNode<K, V>> nodeIterator = this.openIteratorsStack.peek();
				if (nodeIterator.hasNext()) {
					// more child nodes in this node
					HAMTNode<K, V> node = nodeIterator.next();
					if (node.isLeaf()) {
						// iterate over the values
						this.currentEntryIterator = asLeafNode(node).entriesIterator();
						return;
					} else {
						// iterate over its children, continue with first child
						this.openIteratorsStack.push(asIndexNode(node).childNodeIterator());
					}
				} else {
					// no more child nodes on this node, remove it from the stack and continue with the parent node
					this.openIteratorsStack.pop();
				}
			}
			// iterator is done, no more iterators to continue with
			this.currentEntryIterator = null;
			return;
		}

		@Override
		public boolean hasNext() {
			if (this.currentEntryIterator != null && this.currentEntryIterator.hasNext()) {
				// current iterator is not done yet
				return true;
			} else {
				return false;
			}
		}

		@Override
		public Entry<K, V> next() {
			if (this.hasNext() == false) {
				throw new NoSuchElementException("Iterator is exhausted!");
			}
			Entry<K, V> element = this.currentEntryIterator.next();
			this.moveToNextEntryIteratorIfNecessary();
			return element;
		}

	}

	private class KeySetView extends AbstractImmutableSet<K> implements Set<K> {

		@Override
		public int size() {
			return HashArrayMappedTree.this.size;
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean contains(final Object o) {
			if (o == null) {
				throw new NullPointerException("Precondition violation - argument 'o' must not be NULL!");
			}
			return HashArrayMappedTree.this.containsKey((K) o);
		}

		@Override
		public Iterator<K> iterator() {
			return Iterators.transform(HashArrayMappedTree.this.iterator(), entry -> entry.getKey());
		}

	}

	private class ValuesView extends AbstractImmutableCollection<V> implements Collection<V> {

		@Override
		public int size() {
			return HashArrayMappedTree.this.size;
		}

		@Override
		public boolean contains(final Object o) {
			if (o == null) {
				throw new NullPointerException("Precondition violation - argument 'o' must not be NULL!");
			}
			for (Entry<K, V> entry : HashArrayMappedTree.this) {
				if (entry.getValue().equals(o)) {
					return true;
				}
			}
			return false;
		}

		@Override
		public Iterator<V> iterator() {
			return Iterators.transform(HashArrayMappedTree.this.iterator(), entry -> entry.getValue());
		}

	}

	// private static class IndexNodeSpliterator<K, V> implements Spliterator<Entry<K, V>> {
	//
	// private long size = Long.MAX_VALUE;
	// private final boolean isRoot;
	// private final Stack<Iterator<HAMTNode<K, V>>> openNodeIterators;
	//
	// private Iterator<Entry<K, V>> currentEntryIterator = null;
	//
	// public IndexNodeSpliterator(final HashArrayMappedTree<K, V> tree) {
	// this(tree.rootNode, true);
	// }
	//
	// private IndexNodeSpliterator(final Iterator<HAMTNode<K, V>> nodeIterator) {
	// this(nodeIterator, false);
	// }
	//
	// private IndexNodeSpliterator(final HAMTNode<K, V> rootNode, final boolean isRoot) {
	// this(Iterators.singletonIterator(rootNode), isRoot);
	// }
	//
	// private IndexNodeSpliterator(final Iterator<HAMTNode<K, V>> nodeIterator, final boolean isRoot) {
	// this.isRoot = isRoot;
	// this.openNodeIterators = new Stack<>();
	// this.openNodeIterators.push(nodeIterator);
	// this.findNextLeafIterator();
	// }
	//
	// @Override
	// public boolean tryAdvance(final Consumer<? super Entry<K, V>> action) {
	// // current leaf node has more elements
	// if (this.currentEntryIterator != null && this.currentEntryIterator.hasNext()) {
	// Entry<K, V> entry = this.currentEntryIterator.next();
	// action.accept(entry);
	// return true;
	// }
	// // leaf is empty; try to move to next leaf node
	// if (this.openNodeIterators.isEmpty()) {
	// // no more nodes to traverse
	// return false;
	// }
	// this.findNextLeafIterator();
	// if (this.currentEntryIterator == null || this.currentEntryIterator.hasNext() == false) {
	// // empty
	// return false;
	// }
	// Entry<K, V> entry = this.currentEntryIterator.next();
	// action.accept(entry);
	// return true;
	// }
	//
	// private void findNextLeafIterator() {
	// if (this.openNodeIterators.isEmpty()) {
	// // empty
	// this.currentEntryIterator = null;
	// return;
	// }
	// Iterator<HAMTNode<K, V>> iterator = null;
	// while (iterator == null || iterator.hasNext() == false) {
	// if (this.openNodeIterators.isEmpty()) {
	// // empty
	// this.currentEntryIterator = null;
	// return;
	// }
	// iterator = this.openNodeIterators.pop();
	// }
	// // push the started iterator back on to the stack
	// this.openNodeIterators.push(iterator);
	// HAMTNode<K, V> node = iterator.next();
	// while (node.isLeaf() == false) {
	// iterator = asIndexNode(node).childNodeIterator();
	// this.openNodeIterators.push(iterator);
	// node = iterator.next();
	// }
	// this.currentEntryIterator = asLeafNode(node).entriesIterator();
	// }
	//
	// @Override
	// public Spliterator<Entry<K, V>> trySplit() {
	// if (this.openNodeIterators.size() < 2) {
	// return null;
	// }
	// if (this.currentEntryIterator != null && this.currentEntryIterator.hasNext()) {
	// // // we are currently iterating over a leaf node; split off the remainder of this leaf; splitting isn't worth it
	// return null;
	// } else {
	// // our iteration of this leaf node just finished; split off the remainder of this index node
	// if (this.openNodeIterators.isEmpty()) {
	// // empty
	// return null;
	// } else {
	// Iterator<HAMTNode<K, V>> iterator = null;
	// while (iterator == null || iterator.hasNext() == false) {
	// if (this.openNodeIterators.isEmpty()) {
	// // empty
	// return null;
	// }
	// iterator = this.openNodeIterators.pop();
	// }
	// // pop the current iterator, pass it into a child spliterator and continue along
	// Spliterator<Entry<K, V>> childSpliterator = new IndexNodeSpliterator<>(iterator);
	// return childSpliterator;
	// }
	// }
	// }
	//
	// @Override
	// public long estimateSize() {
	// return this.size;
	// }
	//
	// @Override
	// public int characteristics() {
	// int characteristics = IMMUTABLE | NONNULL | DISTINCT;
	// if (this.isRoot) {
	// characteristics = characteristics | SIZED;
	// }
	// return characteristics;
	// }
	//
	// }

}
