package org.chronos.chronodb.internal.util;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Sets;

public class IteratorUtils {

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	public static <T> Iterator<T> unique(final Iterator<T> iterator) {
		checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
		return new UniqueIterator<T>(iterator);
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private static class UniqueIterator<T> implements Iterator<T> {

		private Iterator<T> innerIterator;
		private Set<T> visited;

		private T nextElement;

		public UniqueIterator(final Iterator<T> iterator) {
			checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
			this.innerIterator = iterator;
			this.visited = Sets.newHashSet();
			this.tryFetchNext();
		}

		private void tryFetchNext() {
			this.nextElement = null;
			while (this.innerIterator.hasNext()) {
				T element = this.innerIterator.next();
				if (this.visited.contains(element)) {
					// we have already seen this element; continue with the next one
					continue;
				}
				// this is a new (unseen) element. Register it
				this.visited.add(element);
				this.nextElement = element;
				break;
			}
		}

		@Override
		public boolean hasNext() {
			return this.nextElement != null;
		}

		@Override
		public T next() {
			T current = this.nextElement;
			this.tryFetchNext();
			return current;
		}
	}
}
