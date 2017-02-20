package org.chronos.chronodb.internal.impl.cache.util.lru;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class DefaultUsageRegistry<T> implements UsageRegistry<T> {

	private final Lock lock = new ReentrantLock(true);

	private final Map<Object, Node<T>> valueToNode;
	private Node<T> leastRecentlyUsedNode;
	private Node<T> mostRecentlyUsedNode;

	private final Function<T, Object> topicResolutionFunction;

	private final Set<RemoveListener<T>> globalRemoveListeners;
	private final SetMultimap<Object, RemoveListener<T>> topicToRemoveListeners;

	private boolean isNotifyingListeners;
	private Set<RemoveListener<T>> globalListenersToRemoveAfterNotification = Sets.newHashSet();
	private SetMultimap<Object, RemoveListener<T>> topicListenerstoRemoveAfterNotification = HashMultimap.create();

	public DefaultUsageRegistry(final Function<T, Object> topicResolutionFunction) {
		checkNotNull(topicResolutionFunction, "Precondition violation - argument 'topicResolutionFunction' must not be NULL!");
		this.topicResolutionFunction = topicResolutionFunction;
		this.valueToNode = new ConcurrentHashMap<>();
		this.leastRecentlyUsedNode = null;
		this.mostRecentlyUsedNode = null;
		this.globalRemoveListeners = Sets.newHashSet();
		this.topicToRemoveListeners = HashMultimap.create();
	}

	@Override
	public Function<T, Object> getTopicResolutionFunction() {
		return this.topicResolutionFunction;
	}

	@Override
	public void registerUsage(final T element) {
		if (element == null) {
			throw new IllegalArgumentException("Precondition violation - argument 'element' must not be NULL!");
		}
		this.lock.lock();
		try {
			Node<T> tempNode = this.valueToNode.get(element);
			if (tempNode == null) {
				// this is the first time we see this element; add it as MRU.
				tempNode = new Node<T>(element);
				this.valueToNode.put(element, tempNode);
			}
			// update the position in the list
			this.setMRU(tempNode);
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public int sizeInElements() {
		return this.valueToNode.size();
	}

	@Override
	public void clear() {
		this.lock.lock();
		try {
			this.valueToNode.clear();
			this.mostRecentlyUsedNode = null;
			this.leastRecentlyUsedNode = null;
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public void removeLeastRecentlyUsedElement() {
		this.lock.lock();
		try {
			Node<T> node = this.leastRecentlyUsedNode;
			if (node == null) {
				// no least recently used value -> registry is empty -> nothing to remove
				return;
			}
			T value = node.getValue();
			Object topic = this.topicResolutionFunction.apply(value);
			// remove the element from our value-to-node cache
			Node<T> removedNode = this.valueToNode.remove(value);
			// remove the element from the linked list
			if (this.mostRecentlyUsedNode == this.leastRecentlyUsedNode) {
				// list contains only one item; drop it
				this.mostRecentlyUsedNode = null;
				this.leastRecentlyUsedNode = null;
			} else if (this.mostRecentlyUsedNode.getNext() == this.leastRecentlyUsedNode) {
				// list contains exactly 2 items; drop the latter one
				this.leastRecentlyUsedNode = this.mostRecentlyUsedNode;
				this.mostRecentlyUsedNode.setNext(null);
			} else {
				// the list has more than 2 values
				this.leastRecentlyUsedNode = node.getPrevious();
				if (this.leastRecentlyUsedNode != null) {
					this.leastRecentlyUsedNode.setNext(null);
				}
			}
			// check if an actual remove happened
			if (removedNode == null) {
				// cache content did not change, don't notify listeners
				return;
			}
			this.isNotifyingListeners = true;
			try {
				// notify the "global" listeners (i.e. listeners who don't care about the topic)
				for (RemoveListener<T> listener : this.globalRemoveListeners) {
					listener.objectRemoved(topic, value);
				}
				// notify the topic-based listeners
				for (RemoveListener<T> listener : this.topicToRemoveListeners.get(topic)) {
					listener.objectRemoved(topic, value);
				}
			} finally {
				this.isNotifyingListeners = false;
				// check if there are listeners to remove
				for (RemoveListener<T> listener : this.globalListenersToRemoveAfterNotification) {
					this.globalRemoveListeners.remove(listener);
				}
				this.globalListenersToRemoveAfterNotification.clear();
				for (Entry<Object, RemoveListener<T>> topicListenerEntry : this.topicListenerstoRemoveAfterNotification.entries()) {
					this.topicToRemoveListeners.remove(topicListenerEntry.getKey(), topicListenerEntry.getValue());
				}
				this.topicListenerstoRemoveAfterNotification.clear();
			}
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public void addLeastRecentlyUsedRemoveListener(final Object topic, final RemoveListener<T> listener) {
		checkNotNull(listener, "Precondition violation - argument 'listener' must not be NULL!");
		this.lock.lock();
		try {
			if (topic == null) {
				this.globalRemoveListeners.add(listener);
			} else {
				this.topicToRemoveListeners.put(topic, listener);
			}
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public void removeLeastRecentlyUsedListener(final Object topic, final org.chronos.chronodb.internal.impl.cache.util.lru.UsageRegistry.RemoveListener<T> listener) {
		checkNotNull(listener, "Precondition violation - argument 'listener' must not be NULL!");
		this.lock.lock();
		try {
			if (this.isNotifyingListeners) {
				if (topic == null) {
					this.globalListenersToRemoveAfterNotification.add(listener);
				} else {
					this.topicListenerstoRemoveAfterNotification.put(topic, listener);
				}
			} else {
				if (topic == null) {
					this.globalRemoveListeners.remove(listener);
				} else {
					this.topicToRemoveListeners.remove(topic, listener);
				}
			}
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public int getListenerCount() {
		this.lock.lock();
		try {
			int listeners = this.globalRemoveListeners.size();
			listeners += this.topicToRemoveListeners.size();
			return listeners;
		} finally {
			this.lock.unlock();
		}
	}

	// =====================================================================================================================
	// INTERNAL UTILITY METHODS
	// =====================================================================================================================

	private boolean isMRU(final Node<T> node) {
		if (this.mostRecentlyUsedNode == null) {
			return false;
		}
		return Objects.equal(this.mostRecentlyUsedNode.getValue(), node.getValue());
	}

	private void setMRU(final Node<T> node) {
		if (this.isMRU(node)) {
			// we are already the MRU; do nothing
			return;
		}
		if (this.leastRecentlyUsedNode == node) {
			this.leastRecentlyUsedNode = node.previous;
		}
		node.removeFromList();
		node.setNext(this.mostRecentlyUsedNode);
		this.mostRecentlyUsedNode = node;
		if (node.getNext() == null) {
			// case 1: the list is completely empty. We add the node as first AND last
			this.leastRecentlyUsedNode = node;
			return;
		} else if (node.getNext().getNext() == null) {
			// case 2: the list contained one item. We are the first, and our successor is the last.
			this.leastRecentlyUsedNode = node.getNext();
			return;
		} else {
			// case 3: the list contained more than one item. MRU updates, LRU does not.
			return;
		}
	}

	// @SuppressWarnings("unused") // I leave this method here intact for debugging purposes.
	public String toDebugString() {
		if (this.mostRecentlyUsedNode == null && this.leastRecentlyUsedNode == null) {
			return "[]";
		}
		if (this.mostRecentlyUsedNode == this.leastRecentlyUsedNode) {
			return "[" + this.mostRecentlyUsedNode.getValue() + "]";
		}
		StringBuilder builder = new StringBuilder();
		builder.append("[");
		Node<T> currentNode = this.mostRecentlyUsedNode;
		String separator = "";
		Set<Node<T>> visitedNodes = Sets.newHashSet();
		while (currentNode != null) {
			builder.append(separator);
			separator = "->";
			builder.append(currentNode.getValue());
			if (currentNode.getNext() != null) {
				if (currentNode.getNext().getPrevious() != currentNode) {
					throw new RuntimeException("BROKEN LINK DETECTED. List so far: " + builder.toString());
				}
				if (currentNode.getPrevious() != null) {
					if (currentNode.getPrevious().getNext() != currentNode) {
						throw new RuntimeException("BROKEN LINK DETECTED. List so far: " + builder.toString());
					}
				}
			}
			if (visitedNodes.contains(currentNode)) {
				builder.append("->...");
				throw new RuntimeException("CYCLE DETECTED: " + builder.toString());
			}
			visitedNodes.add(currentNode);
			currentNode = currentNode.getNext();
		}
		builder.append("]");
		return builder.toString();
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	private static class Node<E> {

		private Node<E> previous;
		private Node<E> next;
		private E value;

		public Node(final E value) {
			this(null, null, value);
		}

		public Node(final Node<E> previous, final Node<E> next, final E value) {
			this.previous = previous;
			this.next = next;
			this.value = value;
		}

		public Node<E> getNext() {
			return this.next;
		}

		public Node<E> getPrevious() {
			return this.previous;
		}

		public E getValue() {
			return this.value;
		}

		public void setNext(final Node<E> next) {
			this.next = next;
			if (this.next != null) {
				this.next.previous = this;
			}
		}

		public void removeFromList() {
			if (this.previous != null && this.next != null) {
				// "this" is in the middle of the list
				this.previous.next = this.next;
				this.next.previous = this.previous;
			} else if (this.previous == null && this.next != null) {
				// "this" is at the beginning of the list
				this.next.previous = null;
			} else if (this.previous != null && this.next == null) {
				// "this" is at the end of the list
				this.previous.next = null;
			}
			this.next = null;
			this.previous = null;
		}

	}

}
