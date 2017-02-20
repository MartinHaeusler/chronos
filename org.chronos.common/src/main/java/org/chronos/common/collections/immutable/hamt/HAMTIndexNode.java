package org.chronos.common.collections.immutable.hamt;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.chronos.common.collections.util.BitFieldUtil;

public class HAMTIndexNode<K, V> implements HAMTNode<K, V> {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private HAMTNode<K, V>[] childNodes;
	private int bitfield;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public HAMTIndexNode() {
		this.bitfield = 0;
		this.childNodes = null;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public boolean isLeaf() {
		return false;
	}

	@SuppressWarnings("unchecked")
	public HAMTIndexNode<K, V> attachChild(final int bitFieldIndex, final HAMTNode<K, V> childNode) {
		if (bitFieldIndex < 0 || bitFieldIndex > 31) {
			throw new IllegalArgumentException("Precondition violation - argument 'bitFieldIndex' is out of range!");
		}
		if (childNode == null) {
			throw new NullPointerException("Precondition violation - argument 'childNode' must not be NULL!");
		}
		HAMTIndexNode<K, V> copy = new HAMTIndexNode<>();
		// check that the n'th bit is not yet occupied by another child
		if (BitFieldUtil.getNthBit(this.bitfield, bitFieldIndex)) {
			// this child position is already occupied; this should never happen
			throw new IllegalStateException(
					"Bitfield index " + bitFieldIndex + " is already occupied with a reference to a child node!");
		}
		copy.bitfield = BitFieldUtil.setNthBit(this.bitfield, bitFieldIndex);
		int arrayIndex = BitFieldUtil.popCountUpToBit(this.bitfield, bitFieldIndex - 1);
		// now we need to create the array of references for the copy.
		// in the original, the array looks like this: [child1, child2, child3... child_n]
		// now, we need to insert the new child at the previously calculated index, like so:
		// [child1, child2, ... child_arrayIndex-1, |newChild|, child_arrayIndex+1, ... child_n-1, child_n]
		int arrayLength = 0;
		if (this.childNodes != null) {
			arrayLength = this.childNodes.length;
		}
		// the new copy has one more element in the array than the original
		arrayLength += 1;
		// we have one more child than the original (the one we are inserting)
		copy.childNodes = new HAMTNode[arrayLength];
		// now we need to copy over the references to the child nodes we intend to re-use
		for (int i = 0; i < arrayIndex; i++) {
			copy.childNodes[i] = this.childNodes[i];
		}
		// now we insert our new child
		copy.childNodes[arrayIndex] = childNode;
		// ... and now we copy over the remaining references
		for (int i = arrayIndex + 1; i < arrayLength; i++) {
			copy.childNodes[i] = this.childNodes[i - 1];
		}
		return copy;
	}

	@SuppressWarnings("unchecked")
	public HAMTIndexNode<K, V> detachChild(final int bitFieldIndex) {
		if (bitFieldIndex < 0 || bitFieldIndex > 31) {
			throw new IllegalArgumentException("Precondition violation - argument 'bitFieldIndex' is out of range!");
		}
		// check that the n'th bit is already occupied by a child (otherwise we can't detach)
		if (BitFieldUtil.getNthBit(this.bitfield, bitFieldIndex) == false) {
			// this child position is not occupied and detach is not possible; this should never happen
			throw new IllegalStateException("Bitfield index " + bitFieldIndex
					+ " is not occupied with a reference to a child node, can't detach!");
		}
		HAMTIndexNode<K, V> copy = new HAMTIndexNode<>();
		// clear the child flag
		copy.bitfield = BitFieldUtil.unsetNthBit(this.bitfield, bitFieldIndex);
		// create the new array of children
		if (this.childNodes.length == 1) {
			// only one child existed, which is now gone
			copy.childNodes = null;
		} else {
			// copy the array and skip the entry
			copy.childNodes = new HAMTNode[this.childNodes.length - 1];
			int childIndex = BitFieldUtil.popCountUpToBit(this.bitfield, bitFieldIndex - 1);
			System.arraycopy(this.childNodes, 0, copy.childNodes, 0, childIndex);
			System.arraycopy(this.childNodes, childIndex + 1, copy.childNodes, childIndex,
					this.childNodes.length - childIndex - 1);
		}
		return copy;
	}

	@SuppressWarnings("unchecked")
	public HAMTIndexNode<K, V> replaceChild(final int bitFieldIndex, final HAMTNode<K, V> childNode) {
		if (bitFieldIndex < 0 || bitFieldIndex > 31) {
			throw new IllegalArgumentException("Precondition violation - argument 'bitFieldIndex' is out of range!");
		}
		if (childNode == null) {
			throw new NullPointerException("Precondition violation - argument 'childNode' must not be NULL!");
		}
		HAMTIndexNode<K, V> copy = new HAMTIndexNode<>();
		// check that the n'th bit is already occupied by another child (otherwise we can't replace)
		if (BitFieldUtil.getNthBit(this.bitfield, bitFieldIndex) == false) {
			// this child position is not occupied and replacement is not possible; this should never happen
			throw new IllegalStateException("Bitfield index " + bitFieldIndex
					+ " is not occupied with a reference to a child node, can't replace!");
		}
		// the bitfield remains the same
		copy.bitfield = this.bitfield;
		// duplicate the children
		copy.childNodes = new HAMTNode[this.childNodes.length];
		System.arraycopy(this.childNodes, 0, copy.childNodes, 0, this.childNodes.length);
		// replace the one child entry
		int arrayIndex = BitFieldUtil.popCountUpToBit(this.bitfield, bitFieldIndex - 1);
		copy.childNodes[arrayIndex] = childNode;
		return copy;
	}

	public HAMTNode<K, V> findNextChildNode(final int bitFieldIndex) {
		if (this.bitfield == 0 || this.childNodes == null || this.childNodes.length <= 0) {
			// we do not have any pointer to any child nodes
			return null;
		}
		// check if the n'th bit in our bitmap is set
		if (BitFieldUtil.getNthBit(this.bitfield, bitFieldIndex)) {
			// we have a pointer for that; calculate the child node index
			// by counting the number of 1-bits below our bit index
			int childNodeIndex = BitFieldUtil.popCountUpToBit(this.bitfield, bitFieldIndex - 1);
			// return the child node
			return this.childNodes[childNodeIndex];
		} else {
			// we do not have a pointer
			return null;
		}
	}

	@Override
	public int childNodeCount() {
		if (this.childNodes == null) {
			return 0;
		}
		return this.childNodes.length;
	}

	@Override
	public Iterator<HAMTNode<K, V>> childNodeIterator() {
		if (this.childNodes == null || this.childNodes.length <= 0) {
			return Collections.emptyIterator();
		} else {
			return new ChildNodeIterator();
		}
	}

	@Override
	public int entryCount() {
		return 0;
	}

	@Override
	public Iterator<Entry<K, V>> entriesIterator() {
		return Collections.emptyIterator();
	}

	@Override
	public boolean containsKey(final K key) {
		return false;
	}

	@Override
	public int treeHeight() {
		int maxChildDepth = 0;
		Iterator<HAMTNode<K, V>> childNodeIterator = this.childNodeIterator();
		while (childNodeIterator.hasNext()) {
			HAMTNode<K, V> child = childNodeIterator.next();
			maxChildDepth = Math.max(maxChildDepth, child.treeHeight());
		}
		return maxChildDepth + 1;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("NI");
		builder.append("[");
		builder.append(BitFieldUtil.toBinary(this.bitfield));
		builder.append("]");
		return builder.toString();
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private class ChildNodeIterator implements Iterator<HAMTNode<K, V>> {

		private int index;
		private int length;

		public ChildNodeIterator() {
			this.index = 0;
			this.length = HAMTIndexNode.this.childNodes.length;
		}

		@Override
		public boolean hasNext() {
			return this.index < this.length;
		}

		@Override
		public HAMTNode<K, V> next() {
			if (this.index >= this.length) {
				throw new NoSuchElementException("Iterator is exhausted.");
			}
			HAMTNode<K, V> node = HAMTIndexNode.this.childNodes[this.index];
			this.index++;
			return node;
		}
	}
}
